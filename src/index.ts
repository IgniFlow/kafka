import bodyParser from "body-parser";
import { app, firestore, kafka } from "./config";
import { createConsumer } from "./kafka";

// Insomnia request body
`
localhost:3000/produce

{
  "token": "",
  "team_id": "YHjV3i7ZGSyUuiGDGacu",
  "workspace_id": "NU0gstlKuQfApkYh49mQ",
  "task_title": "Kafka title",
  "task_description": "Kafka description",
  "task_column": "done"
}
`;

// Create messageBuffer for storing messages in create-task consumer
const messagesBuffer: string[] = new Array();

// Setup producer
const producer = kafka.producer();
async function startProducer() {
  await producer.connect();
  console.log("Kafka producer is ready");
}
startProducer().catch(console.error);

// Setup consumers
async function startConsumers() {
  await Promise.all([
    createConsumer({
      groupId: "authenticate-user",
      topic: "authenticate-user",
      messageHandler: async ({ message }) => {
        const parsed = JSON.parse(message.value?.toString() || "{}");

        const { token } = parsed;

        try {
          const decodedUser = { uid: "3NPm7veVIIZpHSpNk4z7oZo3Zvk2" }; //await auth.verifyIdToken(token);
          const timestamp = Date.now().toString();
          console.log("Decoded user", decodedUser.uid);

          await Promise.all([
            producer.send({
              topic: "retrieve-workspace",
              messages: [
                {
                  value: JSON.stringify({
                    ...parsed,
                    user_id: decodedUser.uid,
                    timestamp,
                  }),
                },
              ],
            }),
            producer.send({
              topic: "retrieve-team",
              messages: [
                {
                  value: JSON.stringify({
                    ...parsed,
                    user_id: decodedUser.uid,
                    timestamp,
                  }),
                },
              ],
            }),
          ]);
        } catch (e) {
          console.log(e);
        }
      },
    }),

    createConsumer({
      groupId: "retrieve-workspace",
      topic: "retrieve-workspace",
      messageHandler: async ({ message }) => {
        const parsed = JSON.parse(message.value?.toString() || "{}");
        const { user_id, workspace_id } = parsed;

        const querySnapshot = await firestore
          .collection("workspaces")
          .where("ownerId", "==", user_id)
          .get();
        const workspaces = querySnapshot.docs.map((doc) => {
          return doc.id;
        });

        if (workspaces.includes(workspace_id)) {
          console.log("Found workspace");

          producer.send({
            topic: "create-task",
            messages: [
              {
                value: JSON.stringify(parsed),
              },
            ],
          });
        }
      },
    }),

    createConsumer({
      groupId: "retrieve-team",
      topic: "retrieve-team",
      messageHandler: async ({ topic, message }) => {
        const parsed = JSON.parse(message.value?.toString() || "{}");
        const { user_id, team_id } = parsed;

        const querySnapshot = await firestore
          .collection("teams")
          .where("members", "array-contains", user_id)
          .get();

        const teams = querySnapshot.docs.map((doc) => {
          return doc.id;
        });

        if (teams.includes(team_id)) {
          console.log("Found team");
          producer.send({
            topic: "create-task",
            messages: [
              {
                value: JSON.stringify(parsed),
              },
            ],
          });
        }
      },
    }),

    createConsumer({
      groupId: "create-task",
      topic: "create-task",
      messageHandler: async ({ message }) => {
        const parsed = JSON.parse(message.value?.toString() || "{}");

        const {
          timestamp,
          user_id,
          task_title,
          task_description,
          task_column,
        } = parsed;

        const key = `${user_id}|${timestamp}`;

        if (messagesBuffer.includes(key)) {
          messagesBuffer.splice(messagesBuffer.indexOf(key), 1);

          await firestore.collection("documents").doc().set({
            title: task_title,
            description: task_description,
            column: task_column,
            user: user_id,
          });

          console.log("Task created");
        } else {
          console.log("Message not in buffer");
          messagesBuffer.push(key);
          console.log(messagesBuffer);
        }
      },
    }),
  ]);
}
startConsumers();

// Setup express
const jsonParser = bodyParser.json();
app.post("/produce", jsonParser, async (req, res) => {
  try {
    await producer.send({
      topic: "authenticate-user",
      messages: [{ value: JSON.stringify(req.body) }],
    });
    res.json({ message: "Message sent to Kafka" });
  } catch (error) {
    console.error(`Failed to send message to Kafka: ${error}`);
    res.status(500).json({ error: "Failed to send message to Kafka" });
  }
});
app.listen(3000, () => {
  console.log(`Express server is running on port 3000`);
});
