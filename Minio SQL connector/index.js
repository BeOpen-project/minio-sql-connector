const { Client } = require('pg');
const {minioConfig, postgreConfig } = require('./config')
const minioWriter = require('./minioWriter')

const client = new Client(postgreConfig);
client.connect();

minioWriter.client = client
if (minioConfig.subscribe.all)
    minioWriter.listBuckets().then((buckets) => {
        let a = 1
        for (let bucket of buckets) {
            console.debug("Bucket name : " + bucket.name.toString())
            minioWriter.getNotifications(bucket.name.toString())
            console.debug("Subscribed bucket " + (a++) + " of " + buckets.length)
        }
    })
else
    for (let bucket of minioConfig.subscribe.buckets)
        minioWriter.getNotifications(bucket)

const express = require('express');
const bodyParser = require('body-parser');
const app = express();
const port = 3000;
app.use(bodyParser.json());
app.post('/query', (req, res) => {
    const requestData = req.body.query;
    console.log(requestData)
    let r = res
    client.query(requestData, (err, res) => {
            if (err) {
                console.error("ERROR");
                console.error(err);
                r.status(500).json(err.toString())
                return;
            }
            console.log(res.rows);
            r.send(res.rows)
        });
});
app.listen(port, () => {
    console.log(`Server listens on http://localhost:${port}`);
});