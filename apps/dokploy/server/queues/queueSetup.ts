import {
	execAsync,
	execAsyncRemote,
} from "@dokploy/server/utils/process/execAsync";
import { Queue } from "bullmq";
import { redisConfig } from "./redis-connection";

const myQueue = createQueue("deployments")
const builderServerQueues = []
// Query builder server
const builderServers = await db.query.server.findMany({
	orderBy: desc(server.createdAt),
	where: IS_CLOUD
		? and(
				isNotNull(server.sshKeyId),
				eq(server.organizationId, ctx.session.activeOrganizationId),
				eq(server.serverStatus, "active"),
				eq(server.serverType, "build"),
			)
		: and(
				isNotNull(server.sshKeyId),
				eq(server.organizationId, ctx.session.activeOrganizationId),
				eq(server.serverType, "build"),
			),
});
for(const builderServer of builderServers) {
	builderServerQueues.append(createQueue(`deployments__${builderServer.serverId}`))
}

process.on("SIGTERM", () => {
	myQueue.close();
	for(const builderServerQueue of builderServerQueues) {
		builderServerQueue.close()
	}
	process.exit(0);
});

function createQueue(queueName: string) {
	const queue = new Queue(queueName, {
		connection: redisConfig,
	});
	queue.on("error", (error) => {
		if ((error as any).code === "ECONNREFUSED") {
			console.error(
				"Make sure you have installed Redis and it is running.",
				error,
			);
		}
	});

	return queue
}

export const cleanQueuesByApplication = async (applicationId: string) => {
	const jobs = await myQueue.getJobs(["waiting", "delayed"]);

	for (const job of jobs) {
		if (job?.data?.applicationId === applicationId) {
			await job.remove();
			console.log(`Removed job ${job.id} for application ${applicationId}`);
		}
	}
};

export const cleanQueuesByCompose = async (composeId: string) => {
	const jobs = await myQueue.getJobs(["waiting", "delayed"]);

	for (const job of jobs) {
		if (job?.data?.composeId === composeId) {
			await job.remove();
			console.log(`Removed job ${job.id} for compose ${composeId}`);
		}
	}
};

export const killDockerBuild = async (
	type: "application" | "compose",
	serverId: string | null,
) => {
	try {
		if (type === "application") {
			const command = `pkill -2 -f "docker build"`;

			if (serverId) {
				await execAsyncRemote(serverId, command);
			} else {
				await execAsync(command);
			}
		} else if (type === "compose") {
			const command = `pkill -2 -f "docker compose"`;

			if (serverId) {
				await execAsyncRemote(serverId, command);
			} else {
				await execAsync(command);
			}
		}
	} catch (error) {
		console.error(error);
	}
};

export { myQueue, ...builderServerQueues };
