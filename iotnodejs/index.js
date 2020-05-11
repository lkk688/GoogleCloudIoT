//ref: https://github.com/GoogleCloudPlatform/nodejs-docs-samples/blob/master/iot/mqtt_example/cloudiot_mqtt_example_nodejs.js

'use strict';

// [START iot_mqtt_include]
const fs = require('fs');
const jwt = require('jsonwebtoken');
const mqtt = require('mqtt');
// [END iot_mqtt_include]

// The initial backoff time after a disconnection occurs, in seconds.
const MINIMUM_BACKOFF_TIME = 1;

// The maximum backoff time before giving up, in seconds.
const MAXIMUM_BACKOFF_TIME = 32;

// Whether to wait with exponential backoff before publishing.
let shouldBackoff = false;

// The current backoff time.
let backoffTime = 1;

// Whether an asynchronous publish chain is in progress.
let publishChainInProgress = false;

console.log('Google Cloud IoT Core MQTT nodejs example.');

// Create a Cloud IoT Core JWT for the given project id, signed with the given
// private key.
// [START iot_mqtt_jwt]
const createJwt = (projectId, privateKeyFile, algorithm) => {
    // Create a JWT to authenticate this device. The device will be disconnected
    // after the token expires, and will have to reconnect with a new token. The
    // audience field should always be set to the GCP project id.
    const token = {
        iat: parseInt(Date.now() / 1000),
        exp: parseInt(Date.now() / 1000) + 20 * 60, // 20 minutes
        aud: projectId,
    };
    const privateKey = fs.readFileSync(privateKeyFile);
    return jwt.sign(token, privateKey, { algorithm: algorithm });
};
// [END iot_mqtt_jwt]

// Publish numMessages messages asynchronously, starting from message
// messagesSent.
// [START iot_mqtt_publish]
const publishAsync = (
    mqttTopic,
    client,
    iatTime,
    messagesSent,
    numMessages,
    connectionArgs
) => {
    // If we have published enough messages or backed off too many times, stop.
    if (messagesSent > numMessages || backoffTime >= MAXIMUM_BACKOFF_TIME) {
        if (backoffTime >= MAXIMUM_BACKOFF_TIME) {
            console.log('Backoff time is too high. Giving up.');
        }
        console.log('Closing connection to MQTT. Goodbye!');
        client.end();
        publishChainInProgress = false;
        return;
    }

    // Publish and schedule the next publish.
    publishChainInProgress = true;
    let publishDelayMs = 0;
    if (shouldBackoff) {
        publishDelayMs = 1000 * (backoffTime + Math.random());
        backoffTime *= 2;
        console.log(`Backing off for ${publishDelayMs}ms before publishing.`);
    }

    setTimeout(() => {
        const payload = `${argv.registryId}/${argv.deviceId}-payload-${messagesSent}`;

        // Publish "payload" to the MQTT topic. qos=1 means at least once delivery.
        // Cloud IoT Core also supports qos=0 for at most once delivery.
        console.log('Publishing message:', payload);
        client.publish(mqttTopic, payload, { qos: 1 }, (err) => {
            if (!err) {
                shouldBackoff = false;
                backoffTime = MINIMUM_BACKOFF_TIME;
            }
        });

        const schedulePublishDelayMs = argv.messageType === 'events' ? 1000 : 2000;
        setTimeout(() => {
            // [START iot_mqtt_jwt_refresh]
            const secsFromIssue = parseInt(Date.now() / 1000) - iatTime;
            if (secsFromIssue > argv.tokenExpMins * 60) {
                iatTime = parseInt(Date.now() / 1000);
                console.log(`\tRefreshing token after ${secsFromIssue} seconds.`);

                client.end();
                connectionArgs.password = createJwt(
                    argv.projectId,
                    argv.privateKeyFile,
                    argv.algorithm
                );
                connectionArgs.protocolId = 'MQTT';
                connectionArgs.protocolVersion = 4;
                connectionArgs.clean = true;
                client = mqtt.connect(connectionArgs);
                // [END iot_mqtt_jwt_refresh]

                client.on('connect', (success) => {
                    console.log('connect');
                    if (!success) {
                        console.log('Client not connected...');
                    } else if (!publishChainInProgress) {
                        publishAsync(
                            mqttTopic,
                            client,
                            iatTime,
                            messagesSent,
                            numMessages,
                            connectionArgs
                        );
                    }
                });

                client.on('close', () => {
                    console.log('close');
                    shouldBackoff = true;
                });

                client.on('error', (err) => {
                    console.log('error', err);
                });

                client.on('message', (topic, message) => {
                    console.log(
                        'message received: ',
                        Buffer.from(message, 'base64').toString('ascii')
                    );
                });

                client.on('packetsend', () => {
                    // Note: logging packet send is very verbose
                });
            }
            publishAsync(
                mqttTopic,
                client,
                iatTime,
                messagesSent + 1,
                numMessages,
                connectionArgs
            );
        }, schedulePublishDelayMs);
    }, publishDelayMs);
};
// [END iot_mqtt_publish]

const mqttDeviceDemo = (
    deviceId,
    algorithm,
    privateKeyFile,
    numMessages
) => {
    // [START iot_mqtt_run]

    const projectId = `cmpelkk`;
    //const deviceId = `cmpe181dev1`;
    const registryId = `CMPEIoT1`;
    const region = `us-central1`;
    //const algorithm = `RS256`;
    // const privateKeyFile = `./rsa_private.pem`;
    const mqttBridgeHostname = `mqtt.googleapis.com`;
    const mqttBridgePort = 8883;
    const messageType = `events`;
    //const numMessages = 5;

    // The mqttClientId is a unique string that identifies this device. For Google
    // Cloud IoT Core, it must be in the format below.
    const mqttClientId = `projects/${projectId}/locations/${region}/registries/${registryId}/devices/${deviceId}`;

    // With Google Cloud IoT Core, the username field is ignored, however it must be
    // non-empty. The password field is used to transmit a JWT to authorize the
    // device. The "mqtts" protocol causes the library to connect using SSL, which
    // is required for Cloud IoT Core.
    const connectionArgs = {
        host: mqttBridgeHostname,
        port: mqttBridgePort,
        clientId: mqttClientId,
        username: 'unused',
        password: createJwt(projectId, privateKeyFile, algorithm),
        protocol: 'mqtts',
        secureProtocol: 'TLSv1_2_method',
    };

    // Create a client, and connect to the Google MQTT bridge.
    const iatTime = parseInt(Date.now() / 1000);
    const client = mqtt.connect(connectionArgs);

    // Subscribe to the /devices/{device-id}/config topic to receive config updates.
    // Config updates are recommended to use QoS 1 (at least once delivery)
    client.subscribe(`/devices/${deviceId}/config`, { qos: 1 });

    // Subscribe to the /devices/{device-id}/commands/# topic to receive all
    // commands or to the /devices/{device-id}/commands/<subfolder> to just receive
    // messages published to a specific commands folder; we recommend you use
    // QoS 0 (at most once delivery)
    client.subscribe(`/devices/${deviceId}/commands/#`, { qos: 0 });

    // The MQTT topic that this device will publish data to. The MQTT topic name is
    // required to be in the format below. The topic name must end in 'state' to
    // publish state and 'events' to publish telemetry. Note that this is not the
    // same as the device registry's Cloud Pub/Sub topic.
    const mqttTopic = `/devices/${deviceId}/${messageType}`;

    client.on('connect', (success) => {
        console.log('connect');
        if (!success) {
            console.log('Client not connected...');
        } else if (!publishChainInProgress) {
            publishAsync(mqttTopic, client, iatTime, 1, numMessages, connectionArgs);
            // Publish "payload" to the MQTT topic. qos=1 means at least once delivery.
            // Cloud IoT Core also supports qos=0 for at most once delivery.
            // const payload = `${argv.registryId}/${argv.deviceId}-payload-${messagesSent}`;
            // console.log('Publishing message:', payload);
            // client.publish(mqttTopic, payload, { qos: 1 }, (err) => {
            //     if (!err) {
            //         console.log('Error Publishing message');
            //         shouldBackoff = false;
            //         backoffTime = MINIMUM_BACKOFF_TIME;
            //     }
            // });
        }
    });

    client.on('close', () => {
        console.log('close');
        shouldBackoff = true;
    });

    client.on('error', (err) => {
        console.log('error', err);
    });

    client.on('message', (topic, message) => {
        let messageStr = 'Message received: ';
        if (topic === `/devices/${deviceId}/config`) {
            messageStr = 'Config message received: ';
        } else if (topic.startsWith(`/devices/${deviceId}/commands`)) {
            messageStr = 'Command message received: ';
        }

        messageStr += Buffer.from(message, 'base64').toString('ascii');
        console.log(messageStr);
    });

    client.on('packetsend', () => {
        // Note: logging packet send is very verbose
    });

    // Once all of the messages have been published, the connection to Google Cloud
    // IoT will be closed and the process will exit. See the publishAsync method.
    // [END iot_mqtt_run]
};

//
const { argv } = require(`yargs`)
    .options({
        deviceId: {
            description: 'Cloud IoT device ID.',
            requiresArg: true,
            demandOption: true,
            type: 'string',
        },
        privateKeyFile: {
            description: 'Path to private key file.',
            requiresArg: true,
            demandOption: true,
            type: 'string',
        },
        algorithm: {
            description: 'Encryption algorithm to generate the JWT.',
            requiresArg: true,
            demandOption: true,
            choices: ['RS256', 'ES256'],
            type: 'string',
        },
    })
    .command(
        `mqttDeviceDemo`,
        `Connects a device, sends data, and receives data`,
        {
            numMessages: {
                default: 10,
                description: 'Number of messages to publish.',
                demandOption: true,
                type: 'number',
            },
        },
        (opts) => {
            mqttDeviceDemo(
                opts.deviceId,
                opts.algorithm,
                opts.privateKeyFile,
                opts.numMessages
            );
        }
    )
    .strict();
