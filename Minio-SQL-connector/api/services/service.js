const Source = require('../models/source')
const common = require('../../utils/common')
const { sleep, json2csv } = require('../../utils/common')
const { Client } = require('pg');
const config = require('../../config')
const { minioConfig, postgreConfig, delays, queryAllowedExtensions } = config
const minioWriter = require("../../utils/minioWriter")
const client = new Client(postgreConfig);
client.connect();
minioWriter.client = client
let syncing
if (minioConfig.subscribe.all)
    minioWriter.listBuckets().then((buckets) => {
        let a = 1
        for (let bucket of buckets) {
            minioWriter.getNotifications(bucket.name.toString())
            console.debug("Subscribed bucket " + (a++) + " of " + buckets.length, "(", bucket.name, ")")
        }
    })
else
    for (let bucket of minioConfig.subscribe.buckets)
        minioWriter.getNotifications(bucket)

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
        //console.debug(buckets)
        let bucketIndex = 1
        for (let bucket of buckets) {
            let bucketObjects = await minioWriter.listObjects(bucket.name)
            let index = 1
            for (let obj of bucketObjects) {
                try {
                    await sleep(delays)
                    console.debug("Bucket ", bucketIndex, " of ", buckets.length)
                    console.debug("Scanning object ", index++, " of ", bucketObjects.length, ",", obj.name)
                    let extension = obj.name.split(".").pop()
                    //console.log(queryAllowedExtensions)
                    let isAllowed = (queryAllowedExtensions == "all" || queryAllowedExtensions.includes(extension))
                    if (obj.size && obj.isLatest && isAllowed) {//} && !obj.isDeleteMarker) {
                        let objectGot = await minioWriter.getObject(bucket.name, obj.name, obj.name.split(".").pop())
                        objects.push({ raw: objectGot, info: { ...obj, bucketName: bucket.name } })
                    }
                    else console.log("Size is ", obj.size, ", ", (obj.isLatest ? "is latest" : "is not latest"), " and extension ", (isAllowed ? "is allowed" : "is not allowed"))
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
if (config.syncInterval)
    setInterval(sync, config.syncInterval);

function bucketIs(record, bucket) {
    //console.debug(record, bucket)
    return (record?.s3?.bucket?.name == bucket || record?.bucketName == bucket)
}

function objectFilter(obj, prefix, bucket, visibility) {
    if (visibility == "private" && (obj.record?.name?.includes(prefix) || obj?.name?.includes(prefix))) 
        return true
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

    async save(objects) {
        for (let obj of objects)
            await minioWriter.insertInDBs(obj.raw, obj.info, true)
        return true
    },

    async exampleQueryJson(query) {

        return await Source.find({
            "json": {
                $elemMatch: query
            }
        })
    },

    async exampleQueryGeoJson(query) {

        //let key
        //for (let k in query)
        //    key = k
        let found = []
        //TODO now there is a preset deep level search, but this level should be parametrized
        found.push(
            ...(await Source.find({
                "features": {
                    $elemMatch: {
                        "geometry.coordinates": {
                            $elemMatch: {
                                $elemMatch: {
                                    $elemMatch: {
                                        $elemMatch: {
                                            $eq: Number(query.coordinates) //$elemMatch: { $eq: Number(query[key]) }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            })),
            ...(await Source.find({
                "features": {
                    $elemMatch: {
                        "geometry.coordinates": {
                            $elemMatch: {
                                $elemMatch: {
                                    $elemMatch: {
                                        $eq: Number(query.coordinates) //$elemMatch: { $eq: Number(query[key]) }
                                    }
                                }
                            }
                        }
                    }
                }
            })),
            ...(await Source.find({
                "features": {
                    $elemMatch: {
                        "geometry.coordinates": {
                            $elemMatch: {
                                $elemMatch: {
                                    $eq: Number(query.coordinates) //$elemMatch: { $eq: Number(query[key]) }
                                }
                            }
                        }
                    }
                }
            })),
            ...(await Source.find({
                "features": {
                    $elemMatch: {
                        "geometry.coordinates": {
                            $elemMatch: {
                                $eq: Number(query.coordinates) //$elemMatch: { $eq: Number(query[key]) }
                            }
                        }
                    }
                }
            })),
        )
        return found
    },

    async simpleQuery(query) {
        //console.debug(query)
        let result = await Source.find(query)
        //console.debug(result)
        for (let obj of result) {
            obj.fileName = obj.name.split("/")[obj.name.split("/").lenght - 2]
            obj.path = obj.name
            obj.fileType = obj.name.split(".")[obj.name.split(".").length - 1]
        }
        return result
    },

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
        if (visibility == "public")
            bucket = "public-data"
        for (let obj of await minioWriter.listObjects(bucket)) {
            try {
                if (obj.size && obj.isLatest) {
                    let objectGot = await minioWriter.getObject(bucket, obj.name, obj.name.split(".").pop())
                    objects.push({ raw: objectGot, record: { ...obj, bucketName: bucket }, name: obj.name })
                }
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
                response.send(res.rows.filter(obj => objectFilter(obj, prefix, bucket, visibility)).map(obj => obj.element && obj.name.split(".").pop == "csv" ? {...obj, element : json2csv(obj.element)} : obj))
                console.log(res.rows);
                console.log("Query sql finished")
            }
        });
    }
}