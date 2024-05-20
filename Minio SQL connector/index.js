const { Client } = require('pg');
const { minioConfig, postgreConfig } = require('./config')
const config = require('./config')
const minioWriter = require('./minioWriter')
const common = require('./common')
const service = require('./service/service')

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
const mongoose = require("mongoose");
const cors = require('cors');
const Source = require('./source')
app.use(cors());
//app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(bodyParser.json());
app.post('/query', async (req, res) => {
    const requestData = req.body.query;
    console.log(requestData)
    let r = res
    if (req.query)
        res.send(await Source.find(req.query))
    else
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
app.get('/query', async (req, res) => {
    console.log(req.query)
    res.send(await service.mongoQuery(req.query))
});
app.listen(port, () => {
    console.log(`Server listens on http://localhost:${port}`);
});
mongoose
    .connect(config.mongo, { useNewUrlParser: true })
    .then(() => {
        console.log("Connected to mongo")
    })

function sync() {
    let objects = []
    for (let bucket of minioWriter.listBuckets())
        for (let obj of minioWriter.listObjects(bucket))
            objects.push({ raw: minioWriter.getObject(bucket, obj.name), info: obj })
    service.save(objects)
}

//setInterval(sync, config.syncInterval);
//setInterval(sync, 10000);