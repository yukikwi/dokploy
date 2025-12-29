import { Queue, Worker } from "bullmq";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { db } from "@/server/db";
import {
	deploymentWorkers,
	setupDeploymentWorkers,
} from "@/server/queues/deployments-queue";

// Mock bullmq
vi.mock("bullmq", () => ({
	Queue: vi.fn().mockImplementation((name: string) => ({
		name,
		add: vi.fn(),
		close: vi.fn(),
	})),
	Worker: vi.fn().mockImplementation((name: string, processor: any) => ({
		name,
		processor,
		run: vi.fn(),
		close: vi.fn(),
	})),
}));

// Mock database
vi.mock("@/server/db", () => {
	const createSelectChain = (mockData: any[]) => ({
		from: vi.fn().mockResolvedValue(mockData),
	});

	return {
		db: {
			select: vi.fn(() => createSelectChain([])),
		},
	};
});

// Mock server actions
vi.mock("@dokploy/server", () => ({
	deployApplication: vi.fn(),
	deployCompose: vi.fn(),
	deployPreviewApplication: vi.fn(),
	rebuildApplication: vi.fn(),
	rebuildCompose: vi.fn(),
	updateApplicationStatus: vi.fn(),
	updateCompose: vi.fn(),
	updatePreviewDeployment: vi.fn(),
}));

describe("Deployment queue", () => {
	beforeEach(() => {
		// Clear the deploymentWorkers map before each test
		deploymentWorkers.clear();
		vi.clearAllMocks();
	});

	afterEach(() => {
		deploymentWorkers.clear();
	});

	describe("setupDeploymentWorkers", () => {
		it("should create only local queue and worker when remoteServers return empty array", async () => {
			// Mock empty remote servers array
			const mockSelectChain = {
				from: vi.fn().mockResolvedValue([]),
			};
			vi.mocked(db.select).mockReturnValue(mockSelectChain as any);

			// Execute
			await setupDeploymentWorkers();

			// Verify db query was called
			expect(db.select).toHaveBeenCalled();

			// Verify only "local" worker was created
			expect(deploymentWorkers.size).toBe(1);
			expect(deploymentWorkers.has("local")).toBe(true);

			// Verify Queue and Worker were created for local
			const localWorker = deploymentWorkers.get("local");
			expect(localWorker).toBeDefined();
			expect(localWorker?.queue).toBeDefined();
			expect(localWorker?.worker).toBeDefined();

			// Verify Queue was called with correct name
			expect(Queue).toHaveBeenCalledWith(
				"deployments-local",
				expect.objectContaining({
					connection: expect.any(Object),
				}),
			);

			// Verify Worker was called with correct name
			expect(Worker).toHaveBeenCalledWith(
				"deployments-local",
				expect.any(Function),
				expect.objectContaining({
					autorun: false,
					connection: expect.any(Object),
				}),
			);
		});

		it("should create local and remote servers queue and worker when remoteServers return array of servers", async () => {
			// Mock remote servers array
			const mockServers = [
				{ serverId: "server-1", name: "Server 1" },
				{ serverId: "server-2", name: "Server 2" },
			];

			const mockSelectChain = {
				from: vi.fn().mockResolvedValue(mockServers),
			};
			vi.mocked(db.select).mockReturnValue(mockSelectChain as any);

			// Execute
			await setupDeploymentWorkers();

			// Verify db query was called
			expect(db.select).toHaveBeenCalled();

			// Verify local + 2 remote workers were created
			expect(deploymentWorkers.size).toBe(3);
			expect(deploymentWorkers.has("local")).toBe(true);
			expect(deploymentWorkers.has("server-1")).toBe(true);
			expect(deploymentWorkers.has("server-2")).toBe(true);

			// Verify all workers have queue and worker
			for (const [key, worker] of deploymentWorkers.entries()) {
				expect(worker).toBeDefined();
				expect(worker.queue).toBeDefined();
				expect(worker.worker).toBeDefined();
			}

			// Verify Queue was called for each server
			expect(Queue).toHaveBeenCalledWith(
				"deployments-local",
				expect.objectContaining({
					connection: expect.any(Object),
				}),
			);
			expect(Queue).toHaveBeenCalledWith(
				"deployment-server-1",
				expect.objectContaining({
					connection: expect.any(Object),
				}),
			);
			expect(Queue).toHaveBeenCalledWith(
				"deployment-server-2",
				expect.objectContaining({
					connection: expect.any(Object),
				}),
			);

			// Verify Worker was called for each server
			expect(Worker).toHaveBeenCalledWith(
				"deployments-local",
				expect.any(Function),
				expect.objectContaining({
					autorun: false,
					connection: expect.any(Object),
				}),
			);
			expect(Worker).toHaveBeenCalledWith(
				"deployment-server-1",
				expect.any(Function),
				expect.objectContaining({
					autorun: false,
					connection: expect.any(Object),
				}),
			);
			expect(Worker).toHaveBeenCalledWith(
				"deployment-server-2",
				expect.any(Function),
				expect.objectContaining({
					autorun: false,
					connection: expect.any(Object),
				}),
			);
		});
	});
});
