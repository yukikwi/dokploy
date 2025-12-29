import {
	deployApplication,
	deployCompose,
	deployPreviewApplication,
	rebuildApplication,
	rebuildCompose,
	updateApplicationStatus,
	updateCompose,
	updatePreviewDeployment,
} from "@dokploy/server";
import { type Job, Queue, Worker } from "bullmq";
import { getTableColumns } from "drizzle-orm";
import { db } from "@/server/db";
import { server } from "@/server/db/schema";
import type { DeploymentJob } from "./queue-types";
import { redisConfig } from "./redis-connection";

export const deploymentWorkers: Map<string, { queue: Queue; worker: Worker }> =
	new Map<string, { queue: Queue; worker: Worker }>();

export async function setupDeploymentWorkers() {
	// List build and remote servers
	const remoteServers = await db
		.select({
			...getTableColumns(server),
		})
		.from(server);

	// setup queues and workers
	deploymentWorkers.set("local", createDeploymentQueueAndWorker(null));
	for (const remoteServer of remoteServers) {
		deploymentWorkers.set(
			remoteServer.serverId,
			createDeploymentQueueAndWorker(remoteServer.serverId),
		);
	}
}

function createDeploymentQueueAndWorker(serverId: string | null) {
	const queueName = serverId ? `deployment-${serverId}` : "deployments-local";
	return {
		queue: new Queue(queueName, {
			connection: redisConfig,
		}),
		worker: new Worker(
			queueName,
			async (job: Job<DeploymentJob>) => {
				try {
					if (job.data.applicationType === "application") {
						await updateApplicationStatus(job.data.applicationId, "running");

						if (job.data.type === "redeploy") {
							await rebuildApplication({
								applicationId: job.data.applicationId,
								titleLog: job.data.titleLog,
								descriptionLog: job.data.descriptionLog,
							});
						} else if (job.data.type === "deploy") {
							await deployApplication({
								applicationId: job.data.applicationId,
								titleLog: job.data.titleLog,
								descriptionLog: job.data.descriptionLog,
							});
						}
					} else if (job.data.applicationType === "compose") {
						await updateCompose(job.data.composeId, {
							composeStatus: "running",
						});
						if (job.data.type === "deploy") {
							await deployCompose({
								composeId: job.data.composeId,
								titleLog: job.data.titleLog,
								descriptionLog: job.data.descriptionLog,
							});
						} else if (job.data.type === "redeploy") {
							await rebuildCompose({
								composeId: job.data.composeId,
								titleLog: job.data.titleLog,
								descriptionLog: job.data.descriptionLog,
							});
						}
					} else if (job.data.applicationType === "application-preview") {
						await updatePreviewDeployment(job.data.previewDeploymentId, {
							previewStatus: "running",
						});

						if (job.data.type === "deploy") {
							await deployPreviewApplication({
								applicationId: job.data.applicationId,
								titleLog: job.data.titleLog,
								descriptionLog: job.data.descriptionLog,
								previewDeploymentId: job.data.previewDeploymentId,
							});
						}
					}
				} catch (error) {
					console.log("Error", error);
				}
			},
			{
				autorun: false,
				connection: redisConfig,
			},
		),
	};
}

export const deploymentManagementWorker = new Worker(
	"deployments",
	async (job: Job<DeploymentJob>) => {
		if (job.data.serverId && deploymentWorkers.get(job.data.serverId)) {
			await deploymentWorkers
				.get(job.data.serverId)!
				.queue.add(job.name, job.data, job.opts);
		} else {
			if (await deploymentWorkers.get("local")) {
				await deploymentWorkers
					.get("local")!
					.queue.add(job.name, job.data, job.opts);
			} else {
				console.error("No suitable worker found for job:", job.id);
			}
		}
		job.data;
	},
	{
		autorun: false,
		connection: redisConfig,
	},
);
