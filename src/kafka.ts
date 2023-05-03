import { EachMessagePayload } from "kafkajs";
import { kafka } from "./config";

export async function createConsumer({
  groupId,
  topic,
  messageHandler,
}: {
  groupId: string;
  topic: string;
  messageHandler: (input: EachMessagePayload) => Promise<void>;
}) {
  const consumer = kafka.consumer({ groupId });
  console.log("Creating consumer", topic);

  await consumer.connect();
  await consumer.subscribe({ topic });

  await consumer.run({
    eachMessage: messageHandler,
  });

  console.log("Consumer created", topic);
}
