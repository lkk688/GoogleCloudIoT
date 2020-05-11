'use strict';
const functions = require('firebase-functions');
// Moments library to format dates.
const moment = require('moment');
// CORS Express middleware to enable CORS Requests.
const cors = require('cors')({
    origin: true,
});

// The Firebase Admin SDK to access the Firebase Realtime Database.
const admin = require('firebase-admin');
admin.initializeApp();

// Adds a message that welcomes new users into the chat.
exports.addWelcomeMessages = functions.auth.user().onCreate(async (user) => {
    console.log('A new user signed in for the first time.');
    const fullName = user.displayName || 'Anonymous';

    // Saves the new welcome message into the database
    // which then displays it in the FriendlyChat clients.
    await admin.firestore().collection('messages').add({
        name: 'Firebase Bot',
        profilePicUrl: '/images/firebase-logo.png', // Firebase logo
        text: `${fullName} signed in for the first time! Welcome!`,
        timestamp: admin.firestore.FieldValue.serverTimestamp(),
    });
    console.log('Welcome message written to database.');
});

// Sends a notifications to all users when a new message is posted.
exports.sendNotifications = functions.firestore.document(`messages/{messageId}`).onCreate(
    async (snapshot) => {
        // Notification details.
        const text = snapshot.data().text;
        console.log("snapshot text: " + text);
        const payload = {
            notification: {
                title: `${snapshot.data().name} posted ${text ? 'a message' : 'an image'}`,
                body: text ? (text.length <= 100 ? text : text.substring(0, 97) + '...') : '',
                icon: snapshot.data().profilePicUrl || '/images/profile_placeholder.png',
                click_action: `https://${process.env.GCLOUD_PROJECT}.firebaseapp.com`,
            }
        };

        var tokenlist = [];
        // tokenlist.push('3346dbecf11c0e8a3424be6ea0b8ed10fbcef500ddb63d1413e7bfed97030129');
        // const response = admin.messaging().sendToDevice(tokenlist, payload);
        // console.log('Notifications have been sent and tokens cleaned up.');
        // return;
        admin.firestore().collection('users').get()
            .then((snapshot) => {
                snapshot.forEach((doc) => {
                    console.log(doc.id, '=>', doc.data());
                    tokenlist.push(doc.data().FCMtoken);
                });
                //Send notifications to all tokens.
                const response = admin.messaging().sendToDevice(tokenlist, payload);
                //const tokensToRemove = await cleanupTokens(response, tokens);
                console.log('Notifications have been sent and tokens cleaned up.');
                //res.status(200).send(tokenlist);
                return;
            })
            .catch((err) => {
                console.log('Error getting documents', err);
                //res.status(405).send('Error getting documents', err);
                return;
            });

        // Get the list of device tokens.
        // let dbusers = admin.firestore().collection('users');
        // dbusers.get()
        // .then(doc => {
        //         if (!doc.exists) {
        //             console.log('No such document!');
        //         } else {
        //             console.log('Document data:', doc.data());
        //             const tokens = [];
        //             console.log("FCM tokens:");
        //             doc.forEach((tokenDoc) => {
        //                 //tokens.push(tokenDoc.id);
        //                 console.log(tokenDoc.FCMtoken);
        //                 tokens.push(tokenDoc.FCMtoken);
        //             });
        //             if (tokens.length > 0) {
        //                 console.log("tokens length >0")
        //                 // Send notifications to all tokens.
        //                 const response = await admin.messaging().sendToDevice(tokens, payload);
        //                 const tokensToRemove = await cleanupTokens(response, tokens);
        //                 console.log('Notifications have been sent and tokens cleaned up.');
        //                 // For each message check if there was an error.
        //                 return
        //             } else {
        //                 return console.log('Notification token equal to zero.');
        //             }
        //         }
        //     })
        //     .catch(err => {
        //         console.log('Error getting document', err);
        //         return
        //     });

    });

// Take the text parameter passed to this HTTP endpoint and insert it into the
// Realtime Database under the path /messages/:pushId/original
exports.addMessage = functions.https.onRequest(async (req, res) => {
    // Forbidding PUT requests.
    if (req.method === 'PUT') {
        return res.status(403).send('Forbidden!');
    }
    // Grab the text parameter.
    const original = req.query.text;
    // Push the new message into the Realtime Database using the Firebase Admin SDK.
    const snapshot = await admin.database().ref('/messages').push({ original: original });
    // Redirect with 303 SEE OTHER to the URL of the pushed object in the Firebase console.
    //res.redirect(303, snapshot.ref.toString());
    const format = "MMMM Do YYYY, h:mm:ss a";
    const formattedDate = moment().format(format);
    console.log('Sending Formatted date:', formattedDate);
    console.log('Got query:', original)
    res.status(200).send(formattedDate);
});

exports.backendAPI = functions.https.onRequest(async (req, res) => {
    console.log('Got request:', req.method)
    const queryid = req.query.id;//req.params.id;
    console.log(`Get http query:, ${queryid}`);
    console.log(req.body);
    const bodydata = req.body;//req.rawBody;
    const strData = JSON.stringify(bodydata);
    console.log('Post data body:', strData);


    let db = admin.firestore();
    const dbcollection = 'backend';
    const format = "MMMM Do YYYY, h:mm:ss a";
    const formattedDate = moment().format(format);
    let data = {
        name: queryid,
        payload: bodydata,
        time: formattedDate//new Date()
    };
    switch (req.method) {
        case 'GET':
            if (queryid) {
                let dbRef = db.collection(dbcollection).doc(queryid);
                let getDoc = dbRef.get()
                    .then(doc => {
                        if (!doc.exists) {
                            res.status(200).send('No such document!');
                            console.log('No such document!');
                            return;
                        } else {
                            res.status(200).send(doc.data());
                            console.log('Document data:', doc.data());
                            return;
                        }
                    })
                    .catch(err => {
                        console.log('Error getting document', err);
                        res.status(405).send('Error getting document', err);
                        return;
                    });
            } else {
                //return all data in array
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
                        return;
                    })
                    .catch((err) => {
                        console.log('Error getting documents', err);
                        res.status(405).send('Error getting documents', err);
                        return;
                    });
            }
            break;
        case 'POST':
            console.log('POST');
            db.collection(dbcollection).doc(queryid).set(data);
            res.status(200).json(data)
            break;
        case 'PUT':
            console.log('PUT');
            db.collection(dbcollection).doc(queryid).set(data);
            res.status(200).json(data)
            break;
        case 'DELETE':
            if (!queryid) {
                res.status(405).send('query document id not available');
            } else {
                db.collection(dbcollection).doc(queryid).delete();
                res.status(200).send('Deleted!');
            }
            break;
        default:
            res.status(405).send({ error: 'Something blew up!' });
            break;
    }
});

exports.sendtoTopic = functions.https.onRequest(async (req, res) => {
    console.log('Got request:', req.method)
    const querytopic = req.query.topic;
    console.log(`Get http query:, ${querytopic}`);
    console.log(req.body);
    const bodydata = req.body;//req.rawBody;
    const strData = JSON.stringify(bodydata);
    console.log('Post data body:', strData);

    var topic = querytopic;//'highScores';
    
    if (topic) {
        const payload = {
            notification: {
                title: topic,
                body: strData
            },
            data: //test message data payload
            {
                score: '860',
                time: '2:45'
            },
            topic: `${querytopic}`//topic
        };    
        //Send a message to devices subscribed to the provided topic.
        const response = await admin.messaging().send(payload);
        console.log('Topic notifications have been sent');
        res.status(200).send(response.result);
    } else {
        const payload = {
            notification: {
                title: 'Broadcast',
                body: strData
            },
            data: //test message data payload
            {
                score: '880',
                time: '2:55'
            }
        };   
        var tokenlist = [];
        const dbcollection = 'users';
        admin.firestore().collection(dbcollection).get()
            .then((snapshot) => {
                snapshot.forEach((doc) => {
                    console.log(doc.id, '=>', doc.data());
                    console.log(doc.data().FCMtoken);
                    tokenlist.push(doc.data().FCMtoken);//doc.FCMtoken);
                });
                //Send notifications to all tokens.
                const response = admin.messaging().sendToDevice(tokenlist, payload);
                //const tokensToRemove = await cleanupTokens(response, tokens);
                console.log('Notifications have been sent and tokens cleaned up.');
                res.status(200).send(tokenlist);
                return;
            })
            .catch((err) => {
                console.log('Error getting documents', err);
                res.status(405).send('Error getting documents', err);
                return;
            });
    }

    // if (response.result)
    // {
    //     res.status(500).send(response.result.error);
    // }else{
    //     console.log('Notifications have been sent to topic.');
    //     res.status(200).send(payload);
    // }

});

// Listens for new messages added to /messages/:pushId/original and creates an
// uppercase version of the message to /messages/:pushId/uppercase
exports.makeUppercase = functions.database.ref('/messages/{pushId}/original')
    .onCreate((snapshot, context) => {
        // Grab the current value of what was written to the Realtime Database.
        const original = snapshot.val();
        console.log('Uppercasing', context.params.pushId, original);
        const uppercase = original.toUpperCase();
        // You must return a Promise when performing asynchronous tasks inside a Functions such as
        // writing to the Firebase Realtime Database.
        // Setting an "uppercase" sibling in the Realtime Database returns a Promise.
        return snapshot.ref.parent.child('uppercase').set(uppercase);
    });

// // Create and Deploy Your First Cloud Functions
// // https://firebase.google.com/docs/functions/write-firebase-functions
//
// exports.helloWorld = functions.https.onRequest((request, response) => {
//  response.send("Hello from Firebase!");
// });

// Cleans up the tokens that are no longer valid.
function cleanupTokens(response, tokens) {
    // For each notification we check if there was an error.
    const tokensDelete = [];
    response.results.forEach((result, index) => {
        const error = result.error;
        if (error) {
            console.error('Failure sending notification to', tokens[index], error);
            // Cleanup the tokens who are not registered anymore.
            if (error.code === 'messaging/invalid-registration-token' ||
                error.code === 'messaging/registration-token-not-registered') {
                const deleteTask = admin.firestore().collection('fcmTokens').doc(tokens[index]).delete();
                tokensDelete.push(deleteTask);
            }
        }
    });
    return Promise.all(tokensDelete);
}