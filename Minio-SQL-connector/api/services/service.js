const Source = require('../models/source')
const common = require('../../utils/common')
const { sleep } = require('../../utils/common')
const { Client } = require('pg');
const { minioConfig, postgreConfig, delays } = require('../../config')
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
    return true
}

async function sync() {
    if (!syncing) {
        syncing = true
        let objects = []
        let buckets = await minioWriter.listBuckets()
        console.debug(buckets)
        let bucketIndex = 1
        for (let bucket of buckets) {
            let bucketObjects = await minioWriter.listObjects(bucket.name)
            let index = 1
            for (let obj of bucketObjects) {
                try {
                    await sleep(delays)
                    console.debug("Bucket ", bucketIndex, " of ", buckets.length)
                    console.debug("Getting object ", index++, " of ", bucketObjects.length)
                    if (obj.size && obj.isLatest) {
                        let objectGot = await minioWriter.getObject(bucket.name, obj.name, obj.name.split(".").pop())
                        objects.push({ raw: objectGot, info: { ...obj, bucketName: bucket.name } })
                    }
                    else console.log("Size is ", obj.size, " and "(obj.isLatest ? "is latest" : "is not latest"))
                }
                catch (error) {
                    console.error(error)
                }
            }
            console.debug("Bucket ", bucketIndex++, " of ", buckets.length, " scanning done")
        }
        await save(objects)
        syncing = false
        console.log("Syncing finished")
        return "Sync finished"
    }
    else {
        console.log("Syncing not finished")
        return "Syncing"
    }
}

sync()
//setInterval(sync, 3600000);

function bucketIs(record, bucket) {
    return (record?.s3?.bucket?.name == bucket || record?.bucketName == bucket)
}

let objectFilterCalls = 0

function objectFilter(obj, prefix, bucket, visibility) {
    //console.debug(obj, prefix, bucket, visibility)
    console.debug(++objectFilterCalls)
    if (visibility == "private" && (obj.record?.name?.includes(prefix) || obj?.name?.includes(prefix))) {
        //console.debug(true)
        return true
    }
    if (visibility == "shared" && bucketIs(obj?.record, bucket) && obj?.name?.includes(bucket?.toUpperCase() + " SHARED Data/"))
        return true
    if (visibility == "public" && bucketIs(obj?.record, "public-data"))
        return true
    return false

}

module.exports = {

    async exampleQueryCSV(query) {

        return await Source.find({
            "csv": {
                $elemMatch: query
            }
        })
    },

    sync: sync,

    async minioListObjects(bucketName) {
        return await minioWriter.listObjects(bucketName)
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
        return true
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

    async simpleQuery(query) {
        let result = await Source.find(query)
        for (let obj of result) {
            obj.fileName = obj.name.split("/")[obj.name.split("/").lenght - 2]
            obj.path = obj.name
            obj.fileType = obj.name.split(".")[obj.name.split(".").length - 1]
            console.debug(obj.path)
        }
        return result
    },

    //fileName
    //path 
    //fileType

    async mongoQuery(query, prefix, bucket, visibility) {
        let format = query.format?.toLowerCase()
        if (format)
            delete query["format"]
        //query.name = new RegExp("^" + prefix, 'i')
        switch (format) {
            case "geojson": return (await this.exampleQueryGeoJson(query)).filter(obj => objectFilter(obj, prefix, bucket, visibility))//obj.name.includes(prefix))
            case "csv": return (await this.exampleQueryCSV(query)).filter(obj => objectFilter(obj, prefix, bucket, visibility))
            case "json": return (await this.exampleQueryJson(query)).filter(obj => objectFilter(obj, prefix, bucket, visibility))
            case "object": return (await this.simpleQuery(query)).filter(obj => objectFilter(obj, prefix, bucket, visibility))
            default: return (await this.simpleQuery(query)).filter(obj => objectFilter(obj, prefix, bucket, visibility))
        }
    },

    async rawQuery(query, prefix, bucket, visibility) {
        //query.name = new RegExp("^" + prefix, 'i')
        let objects = []
        for (let obj of await minioWriter.listObjects(bucket)) {
            try {
                let objectGot = await minioWriter.getObject(bucket, obj.name, obj.name.split(".").pop())
                objects.push({ raw: objectGot, record: { ...obj, bucketName: bucket }, name: obj.name })
            }
            catch (error) {
                console.error(error)
            }
        }
        return objects.filter(obj => typeof obj.raw == "string" ? objectFilter(obj, prefix, bucket, visibility) && obj.raw.includes(query.value) : objectFilter(obj, prefix, bucket, visibility) && JSON.stringify(obj.raw).includes(query.value))
        //return objects.filter(obj => typeof obj.raw == "string" ? obj.record.name.includes(prefix) && obj.raw.includes(query.value) : obj.record.name.includes(prefix) && JSON.stringify(obj.raw).includes(query.value))
    },


    querySQL(response, query, prefix, bucket, visibility) {
        client.query(query, (err, res) => {
            if (err) {
                console.error("ERROR");
                console.error(err);
                response.status(500).json(err.toString())
                console.log("Query sql finished with errors")
                return;
            }
            else {
                //if (visibility == "shared")
                //    prefix = bucket + " " 
                response.send(res.rows.filter(obj => objectFilter(obj, prefix, bucket, visibility)))
                console.log(res.rows);
                console.log("Query sql finished")
            }
        });
    }
}