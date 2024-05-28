const Source = require('../models/source')
const common = require('../../utils/common')
const { Client } = require('pg');
const { minioConfig, postgreConfig } = require('../../config')
const minioWriter = require("../../utils/minioWriter")
const client = new Client(postgreConfig);
client.connect();
minioWriter.client = client
if (minioConfig.subscribe.all)
    minioWriter.listBuckets().then((buckets) => {
        let a = 1
        for (let bucket of buckets) {
            console.debug("Bucket name : " + bucket.name.toString())
            //minioWriter.subscribe(bucket.name.toString())
            //minioWriter.setNotifications(bucket.name.toString())
            minioWriter.getNotifications(bucket.name.toString())
            console.debug("Subscribed bucket " + (a++) + " of " + buckets.length)
        }
    })
else
    for (let bucket of minioConfig.subscribe.buckets)
        minioWriter.getNotifications(bucket)

let syncing

async function save(objects) {
    for (let obj of objects)
        await minioWriter.insertInDBs(obj.raw, obj.info, true)
}

async function sync() {
    if (!syncing) {
        syncing = true
        let objects = []
        console.debug(await minioWriter.listBuckets())
        for (let bucket of await minioWriter.listBuckets())
            for (let obj of await minioWriter.listObjects(bucket.name))
                objects.push({ raw: await minioWriter.getObject(bucket.name, obj.name, obj.name.split(".").pop()), info: { ...obj, bucketName: bucket.name } })
        await save(objects)
        syncing = false
    }
    else console.log("Syncing not finished")
}

sync()
setInterval(sync, 3600000);

module.exports = {

    async exampleQueryCSV(query) {

        return await Source.find({
            "csv": {
                $elemMatch: query
            }
        })
    },

    async save0(objects) {

        await Source.deleteMany({ "record.s3.bucket.name": bucket })
        // TRUNCATE TABLE users;
        this.client.query("TRUNCATE TABLE " + bucket, (err, res) => {
            if (err) {
                console.error("ERROR deleting object in DB");
                console.error(err);
                return;
            }
            console.log("Objects deleted ", res?.rows);
        });
        for (let record of objects)
            this.client.query(`INSERT INTO ${bucket} (name, data) VALUES ('${record.s3.object.key}', '${jsonStringified || common.cleaned(newObject)}')`, (err, res) => {
                if (err) {
                    console.error("ERROR inserting object in DB");
                    console.error(err);
                    return;
                }
                console.log("Object inserted \n");

                //console.log("Object inserted \n", res.rows);
            });
        return await Source.insertMany(objects)
    },

    async save(objects) {
        for (let obj of objects)
            await minioWriter.insertInDBs(obj.raw, obj.info, true)
        //await Source.findOneAndUpdate({"record.s3.object.key" : obj.record.s3.object.key}, obj)
    },

    async exampleQueryJson(query) {

        return await Source.find({
            "json": {
                $elemMatch: query
            }
        })
    },

    async exampleQueryGeoJson(query) {

        let key

        for (let k in query)
            key = k

        console.debug(query[key])

        return await Source.find({
            "features": {
                $elemMatch: {
                    //"a": {
                    "geometry.coordinates": {
                        $elemMatch: {
                            $elemMatch: {

                                $elemMatch: {
                                    $elemMatch: { $eq: Number(query[key]) }
                                }
                            }
                        }
                    }
                }
            }
        })
    },

    async mongoQuery(query, prefix) {
        let format = query.format?.toLowerCase()
        if (format)
            delete query["format"]
        query.name = new RegExp("^" + prefix, 'i')
        switch (format) {
            case "geojson": return await this.exampleQueryGeoJson(query)
            case "csv": return await this.exampleQueryCSV(query)
            case "json": return this.exampleQueryJson(query)
            case "object": return await Source.find(query)
            default: return await Source.find(query)
        }
    },

    querySQL(response, query, prefix) {
        client.query(query, (err, res) => {
            if (err) {
                console.error("ERROR");
                console.error(err);
                response.status(500).json(err.toString())
                return;
            }
            else
                response.send(res.rows)
            console.log(res.rows);
        });
    }
}