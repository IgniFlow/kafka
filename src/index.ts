import bodyParser from 'body-parser';
import cors from 'cors';
import { app, auth, firestore, kafka } from './config';
import { createConsumer } from './kafka';
// Create messageBuffer for storing messages in create-task consumer
const messagesBuffer: string[] = new Array();

// Setup producer
const producer = kafka.producer();
async function startProducer() {
    await producer.connect();
    console.log('Kafka producer is ready');
}
startProducer().catch(console.error);

// Setup consumers
async function startConsumers() {
    await Promise.all([
        createConsumer({
            groupId: 'authenticate-user',
            topic: 'authenticate-user',
            messageHandler: async ({ message }) => {
                const parsed = JSON.parse(message.value?.toString() || '{}');

                const { token } = parsed;

                try {
                    const decodedUser = await auth.verifyIdToken(token);
                    const timestamp = Date.now().toString();
                    console.log('Decoded user', decodedUser.uid);

                    await Promise.all([
                        producer.send({
                            topic: 'retrieve-workspace',
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
                            topic: 'retrieve-team',
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
            groupId: 'retrieve-workspace',
            topic: 'retrieve-workspace',
            messageHandler: async ({ message }) => {
                const parsed = JSON.parse(message.value?.toString() || '{}');
                const { user_id, workspace_id } = parsed;

                const querySnapshot = await firestore
                    .collection('workspaces')
                    .where('ownerId', '==', user_id)
                    .get();
                const workspaces = querySnapshot.docs.map((doc) => {
                    return doc.id;
                });

                if (workspaces.includes(workspace_id)) {
                    console.log('Found workspace');

                    producer.send({
                        topic: 'create-task',
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
            groupId: 'retrieve-team',
            topic: 'retrieve-team',
            messageHandler: async ({ topic, message }) => {
                const parsed = JSON.parse(message.value?.toString() || '{}');
                const { user_id, team_id } = parsed;

                const querySnapshot = await firestore
                    .collection('teams')
                    .where('members', 'array-contains', user_id)
                    .get();

                const teams = querySnapshot.docs.map((doc) => {
                    return doc.id;
                });

                if (teams.includes(team_id)) {
                    console.log('Found team');
                    producer.send({
                        topic: 'create-task',
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
            groupId: 'create-task',
            topic: 'create-task',
            messageHandler: async ({ message }) => {
                const parsed = JSON.parse(message.value?.toString() || '{}');

                const {
                    workspace_id,
                    document_id,
                    timestamp,
                    user_id,
                    task_title,
                    task_description,
                    task_column,
                } = parsed;

                const key = `${user_id}|${timestamp}`;

                if (messagesBuffer.includes(key)) {
                    messagesBuffer.splice(messagesBuffer.indexOf(key), 1);

                    await firestore.collection('documents').add({
                        workspaceId: workspace_id,
                        documentId: document_id,
                        type: 'item',
                        views: null,
                        title: task_title,
                        content: task_description,
                        ownerId: user_id,
                        properties: {
                            status: task_column,
                            assignee: [],
                        },
                    });

                    console.log('Task created');
                } else {
                    console.log('Message not in buffer');
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
app.use(cors());
app.post('/produce', jsonParser, async (req, res) => {
    try {
        await producer.send({
            topic: 'authenticate-user',
            messages: [{ value: JSON.stringify(req.body) }],
        });
        res.json({ message: 'Message sent to Kafka' });
    } catch (error) {
        console.error(`Failed to send message to Kafka: ${error}`);
        res.status(500).json({ error: 'Failed to send message to Kafka' });
    }
});
app.listen(3001, () => {
    console.log(`Express server is running on port 3001`);
});
