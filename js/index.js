const {PubSub} = require('@google-cloud/pubsub');
const pubsub = new PubSub();

const express = require('express');
const app = express();

const TOPIC = `projects/${process.env.PROJECT}/topics/${process.env.TOPIC}`;
const publisher = pubsub.topic(TOPIC).publisher();
const message1 = JSON.stringify({"EventID":"12","HomeID":"22222","SmartThingsSensorName":"sa#door#1#front_door","State":"closed","Label":null,"HasBeenLabelled":null,"EventTime":"2019-01-09T12:22:22Z","CreateDate":"2019-01-09T15:17:00Z","ModifyDate":"2019-01-09T15:17:00Z"});
const message2 = JSON.stringify({"EventID":"12","HomeID":"22222","SmartThingsSensorName":"noiseFloor","State":"closed","Label":null,"HasBeenLabelled":null,"EventTime":"2019-01-09T12:22:22Z","CreateDate":"2019-01-09T15:17:00Z","ModifyDate":"2019-01-09T15:17:00Z"});
const messages = [message1, message2];

app.get('/', async (req, res) => {
    res.status(200).send('Hello World');
});

app.get('/publish', async (req, res, next) => {
    let message = messages[Math.floor(Math.random()*2)];
    const dataBuffer = Buffer.from(message);
    try {
        let messageId = await publisher.publish(dataBuffer);
        res.status(200).send(`Message ${messageId} sent.`);
    } catch (error){
        next(error);
    }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
    console.log(`App listening on port ${PORT}`);
    console.log('Press Ctrl+C to quit.');
});

module.exports = app;