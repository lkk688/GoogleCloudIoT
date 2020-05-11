
//ref: https://github.com/googleapis/nodejs-firestore
const Firestore = require('@google-cloud/firestore');
// const db = new Firestore({
//     projectId: 'cmpelkk',
//     keyFilename: '~/Documents/GoogleCloud/certs/cmpelkk-380e31c10ee7.json',
// });
const db = new Firestore();
const dbcollection = 'iottests';

// Imports the Google Cloud client library
const { BigQuery } = require('@google-cloud/bigquery');
// Instantiates a client
const bigquery = new BigQuery({
    projectId: 'cmpelkk'
});

async function testfirestore() {
    // Obtain a document reference.
    const document = db.doc('iottests/intro-to-firestore');

    // Enter new data into the document.
    await document.set({
        title: 'Welcome to Firestore',
        body: 'Hello World',
    });
    console.log('Entered new data into the document');

    // Update an existing document.
    await document.update({
        body: 'My first Firestore app',
    });
    console.log('Updated an existing document');

    // Read the document.
    let doc = await document.get();
    console.log('Read the document');

    // Delete the document.
    await document.delete();
    console.log('Deleted the document');
}
testfirestore();

// Get a reference to the Cloud Storage component
const { Storage } = require('@google-cloud/storage');
const storage = new Storage();

// Imports the Google Cloud Vision client library
const vision = require('@google-cloud/vision');
// Creates a cloud vision client
const visionclient = new vision.ImageAnnotatorClient();
async function cloudvisiontest() {
    // Performs label detection on the image file
    const [result] = await visionclient.labelDetection('./resources/SF1.jpg');//wakeupcat.jpg
    const labels = result.labelAnnotations;
    console.log('Labels:');
    const resultlist = [];
    labels.forEach(label => {
        resultlist.push(label.description);
        //console.log(label.description);
    });
    console.log(resultlist);
    //return publishResult("ML_RESULT", resultlist);
}
cloudvisiontest();

// Get a reference to the Pub/Sub component
const { PubSub } = require('@google-cloud/pubsub');
const pubsub = new PubSub();
// [START functions_ocr_publish]
/**
 * Publishes the result to the given pubsub topic and returns a Promise.
 *
 * @param {string} topicName Name of the topic on which to publish.
 * @param {object} data The message data to publish.
 */
const publishResult = async (topicName, data) => {
    const dataBuffer = Buffer.from(JSON.stringify(data));
    console.log("publishResult");
    const [topic] = await pubsub.topic(topicName).get({ autoCreate: true });
    topic.publish(dataBuffer)
};
// [END functions_ocr_publish]


/**
 * Background Cloud Function to be triggered by Cloud Storage.
 *
 * @param {object} data The event payload.
 * @param {object} context The event metadata.
 */
//ref: https://github.com/GoogleCloudPlatform/nodejs-docs-samples/blob/master/functions/helloworld/index.js
// detect uploaded images that are flagged as Adult or Violence.
exports.processImagesonGCS = async (data, context) => {
    const file = data;

    console.log(`  Event: ${context.eventId}`);
    console.log(`  Event Type: ${context.eventType}`);
    console.log(`  Bucket: ${file.bucket}`);
    console.log(`  File: ${file.name}`);
    console.log(`  Metageneration: ${file.metageneration}`);
    console.log(`  Created: ${file.timeCreated}`);
    console.log(`  Updated: ${file.updated}`);


    const filePath = `gs://${file.bucket}/${file.name}`;
    console.log(`Analyzing ${filePath}.`);

    const filestote = storage.bucket(file.bucket).file(file.name);
    console.log(`Analyzing storage bucket: ${filestote.name}.`);

    try {
        console.log("Start vision API.")
        const [result] = await visionclient.safeSearchDetection(filePath);
        const detections = result.safeSearchAnnotation || {};
        if (
            // Levels are defined in https://cloud.google.com/vision/docs/reference/rest/v1/AnnotateImageResponse#likelihood
            detections.adult === 'VERY_LIKELY' ||
            detections.violence === 'VERY_LIKELY'
        ) {
            console.log(`Detected ${file.name} as inappropriate.`);
            //return blurImage(file, BLURRED_BUCKET_NAME);
        } else {
            console.log(`Detected ${file.name} as OK.`);
        }
    } catch (err) {
        console.error(`Failed to analyze ${file.name}.`, err);
        throw err;
    }
    //return
};

//gcloud pubsub topics publish cmpeiotdevice1 --message "test pubsub"
/**
 * Background Cloud Function to be triggered by Pub/Sub.
 * This function is exported by index.js, and executed when
 * the trigger topic receives a message.
 *
 * @param {object} data The event payload.
 * @param {object} context The event metadata.
 */
exports.iotPubSub = async (data, context) => {
    const pubSubMessage = data;
    const name = pubSubMessage.data
        ? Buffer.from(pubSubMessage.data, 'base64').toString()
        : 'World';

    console.log(`Hello, ${name}!`);

    const iotdata = JSON.parse(name);
    console.log(iotdata.registry_id);
    //  "Hello, {"registry_id": "CMPEIoT1", "device_id": "cmpe181dev1", "timecollected": "2020-04-27 02:00:21", "zipcode": "94043", "latitude": "37.421655", "longitude": "-122.085637", "temperature": "25.15", "humidity": "78.93", "image_file": "img9.jpg"}!"   

    // Inserts data into a bigquery table
    var rows = [iotdata];
    console.log(`Uploading data to bigquery: ${JSON.stringify(rows)}`);
    bigquery
        .dataset('iottest')
        .table('test1')
        .insert(rows)
        .then((foundErrors) => {
            rows.forEach((row) => console.log('Inserted: ', row));

            if (foundErrors && foundErrors.insertErrors != undefined) {
                foundErrors.forEach((err) => {
                    console.log('Bigquery Error: ', err);
                })
            }
        })
        .catch((err) => {
            console.error('Bigquery ERROR:', err);
        });


    //Access the file in Google cloud storage
    bucketname = 'cmpelkk_imagetest';
    const filePath = `gs://${bucketname}/${iotdata.image_file}`;
    console.log(`Analyzing ${filePath}.`);

    const filestote = storage.bucket(bucketname).file(iotdata.image_file);
    console.log(`Analyzing storage bucket: ${filestote.name}.`);

    console.log("Start vision API.")
    const [result] = await visionclient.labelDetection(filePath);
    const labels = result.labelAnnotations;
    console.log('Labels:');
    const labellist = [];
    labels.forEach(label => {
        console.log(label.description);
        labellist.push(label.description);
    });

    let dbcol = 'IoTdb';
    // Add a new document with a generated id.
    let addDoc = db.collection(dbcol).add({
        registry_id: iotdata.registry_id,
        device_id: iotdata.device_id,
        image_file: iotdata.image_file,
        sensorpayload: iotdata,
        labels: labellist,
        time: new Date()
    }).then(ref => {
        console.log('Added document with ID: ', iotdata.device_id);
    });


    // try {
    //     console.log("Start vision API.")
    //     const [result] = await visionclient.safeSearchDetection(filePath);
    //     const detections = result.safeSearchAnnotation || {};
    //     if (
    //         // Levels are defined in https://cloud.google.com/vision/docs/reference/rest/v1/AnnotateImageResponse#likelihood
    //         detections.adult === 'VERY_LIKELY' ||
    //         detections.violence === 'VERY_LIKELY'
    //     ) {
    //         console.log(`Detected ${iotdata.image_file} as inappropriate.`);
    //         //return blurImage(file, BLURRED_BUCKET_NAME);
    //     } else {
    //         console.log(`Detected ${iotdata.image_file} as OK.`);
    //     }
    // } catch (err) {
    //     console.error(`Failed to analyze ${iotdata.image_file}.`, err);
    //     throw err;
    // }
};


const escapeHtml = require('escape-html');
//curl -X POST HTTP_TRIGGER_ENDPOINT -H "Content-Type:application/json"  -d '{"name":"Jane"}'
/**
 * HTTP Cloud Function.
 *
 * @param {Object} req Cloud Function request context.
 *                     More info: https://expressjs.com/en/api.html#req
 * @param {Object} res Cloud Function response context.
 *                     More info: https://expressjs.com/en/api.html#res
 */
exports.httpApi = (req, res) => {
    const id = req.query.id;//req.params.id;
    console.log(`Get http query:, ${id}`);
    let bodydata;

    switch (req.get('content-type')) {
        // '{"name":"John"}'
        case 'application/json':
            ({ bodydata } = req.body);
            console.log("json content", JSON.stringify(bodydata));
            break;

        // 'John', stored in a Buffer
        case 'application/octet-stream':
            bodydata = req.body.toString(); // Convert buffer to a string
            break;

        // 'John'
        case 'text/plain':
            bodydata = req.body;
            break;

        // 'name=John' in the body of a POST request (not the URL)
        case 'application/x-www-form-urlencoded':
            ({ bodydata } = req.body);
            break;
    }
    //bodydata =req.body
    if (bodydata) {
        console.log(`Get body query:, ${bodydata}`);
    } else {
        bodydata = req.body ? req.body : 'nobody';
        console.log(`No body query: ${bodydata}`);
        console.log(bodydata);
    }
    switch (req.method) {
        case 'GET':
            const datalist = [];
            db.collection(dbcollection).get()
                .then((snapshot) => {
                    snapshot.forEach((doc) => {
                        console.log(doc.id, '=>', doc.data());
                        datalist.push({
                            id: doc.id,
                            data: doc.data()
                        });
                    });
                    res.status(200).send(datalist);
                })
                .catch((err) => {
                    console.log('Error getting documents', err);
                    res.status(405).send('Error getting data');
                });
            //res.status(200).send('Get request!');
            break;
        case 'POST':
            let docRef = db.collection(dbcollection).doc(id);
            console.log('POST Created doc ref');
            let setdoc = docRef.set({
                name: id,
                sensors: bodydata,
                time: new Date()
            });
            //res.status(200).send('Get Post request!');
            //sendCommand('cmpe181dev1', 'CMPEIoT1', 'cmpelkk', 'us-central1', 'test command');
            //res.status(200).send(`Post data: ${escapeHtml(bodydata || 'World')}!`);
            res.status(200).json({
                name: id,
                sensors: bodydata,
                time: new Date()
            })
            break;
        case 'PUT':
            let docdelRef = db.collection(dbcollection).doc(id);
            console.log('PUT Created doc ref');
            let deldoc = docdelRef.set({
                name: id,
                sensors: bodydata,
                time: new Date()
            });
            res.status(200).json({
                name: id,
                sensors: bodydata,
                time: new Date()
            })
            //res.status(403).send('Forbidden!');
            break;
        case 'DELETE':
            if (!id) {
                res.status(405).send('query document id not available');
            } else {
                let deleteDoc = db.collection(dbcollection).doc(id).delete();
                res.status(200).send('Deleted!');
            }
            break;
        default:
            res.status(405).send({ error: 'Something blew up!' });
            break;
    }
    //res.send(`Hello ${escapeHtml(req.query.name || req.body.name || 'World')}!`);
};

exports.securehttpApi = async (req, res) => {
    const id = req.query.id;
    console.log(`Get http query:, ${id}`);
    let bodydata = req.body ? req.body : 'nobody';
    console.log('Get http body:');
    console.log(bodydata);
    publishResult("ML_RESULT", "Get body");
    switch (req.method) {
        case 'GET':
            console.log("Get request");
            publishResult("ML_RESULT", "Get request");
            res.status(200).send('Get secure request!');
            break;
        case 'POST':
            console.log("Get post");
            publishResult("ML_RESULT", "Get POST");
            await detectText('cmpelkk_imagetest', 'sjsu.jpg');
            res.status(200).send('Get POST request!');
            break;
        default:
            res.status(405).send({ error: 'Something blew up!' });
            break;
    }
}

const sendCommand = async (
    deviceId,
    registryId,
    projectId,
    cloudRegion,
    commandMessage
) => {
    // [START iot_send_command]
    // const cloudRegion = 'us-central1';
    // const deviceId = 'my-device';
    // const commandMessage = 'message for device';
    // const projectId = 'adjective-noun-123';
    // const registryId = 'my-registry';
    const iot = require('@google-cloud/iot');
    const iotClient = new iot.v1.DeviceManagerClient({
        // optional auth parameters.
    });

    const formattedName = iotClient.devicePath(
        projectId,
        cloudRegion,
        registryId,
        deviceId
    );
    const binaryData = Buffer.from(commandMessage);
    const request = {
        name: formattedName,
        binaryData: binaryData,
    };

    try {
        const responses = await iotClient.sendCommandToDevice(request);
        console.log('Sent command: ', responses[0]);
    } catch (err) {
        console.error('Could not send command:', err);
    }
    // [END iot_send_command]
};

const detectText = async (bucketName, filename) => {
    console.log(`Looking for text in image ${filename}`);
    const [textDetections] = await visionclient.textDetection(
        `gs://${bucketName}/${filename}`
    );
    const [annotation] = textDetections.textAnnotations;
    const text = annotation ? annotation.description : '';
    console.log(`Extracted text from image:`, text);

    return publishResult("ML_RESULT", text);

};