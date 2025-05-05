const Log = require('../../utils/logger')//.app(module);
const { Logger } = Log
const logger = new Logger("service")
const log = logger.info
const Source = require('../models/source')
const Value = require('../models/value')
const Key = require('../models/key')
const Entries = require('../models/entries')
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
            logger.debug("Subscribed bucket " + (a++) + " of " + buckets.length, "(", bucket.name, ")")
        }
    })
else
    for (let bucket of minioConfig.subscribe.buckets)
        minioWriter.getNotifications(bucket)

async function sync() {
    try {
        if (!syncing) {
            syncing = true
            await Source.deleteMany({})
            await Key.deleteMany({})
            await Value.deleteMany({})
            await Entries.deleteMany({})
            let objects = []
            let buckets = await minioWriter.listBuckets()
            let bucketIndex = 1
            for (let bucket of buckets) {
                let bucketObjects = await minioWriter.listObjects(bucket.name)
                let index = 1
                for (let obj of bucketObjects) {
                    try {
                        //await sleep(delays)
                        logger.debug("Bucket ", bucketIndex, " of ", buckets.length)
                        logger.debug("Scanning object ", index++, " of ", bucketObjects.length, ",", obj.name)
                        let extension = obj.name.split(".").pop()
                        let isAllowed = (queryAllowedExtensions == "all" || queryAllowedExtensions.includes(extension))
                        if (obj.size && obj.isLatest && isAllowed) {//} && !obj.isDeleteMarker) {
                            let objectGot = await minioWriter.getObject(bucket.name, obj.name, obj.name.split(".").pop())
                            objects.push({ raw: objectGot, info: { ...obj, bucketName: bucket.name } })
                        }
                        else logger.info("Size is ", obj.size, ", ", (obj.isLatest ? "is latest" : "is not latest"), " and extension ", (isAllowed ? "is allowed" : "is not allowed"))
                    }
                    catch (error) {
                        logger.error(error)
                    }
                }
                logger.debug("Bucket ", bucketIndex++, " of ", buckets.length, " scanning done")
            }

            minioWriter.entities.values = []
            minioWriter.entities.keys = []
            minioWriter.entities.entries = []
            minioWriter.entities.uniqueValues = []
            minioWriter.entities.uniqueKeys = []
            minioWriter.entities.uniqueEntries = []

            for (let obj of objects)
                try {
                    await minioWriter.insertInDBs(obj.raw, obj.info, true)
                }
                catch (error) {
                    logger.error(error)
                }

            let entries = Object.entries(minioWriter.entries).map(([key, value]) => ({ [key]: value }));
            let entriesInDB = []
            for (let key in minioWriter.entries)
                for (let value in minioWriter.entries[key])
                    entriesInDB.push({
                        key,
                        value,
                        visibility: minioWriter.entries[key][value]
                    })
            try {
                if (entriesInDB.length > 0) await Entries.insertMany(entriesInDB);
            } catch (error) {
                if (!error?.errorResponse?.message?.includes("Document can't have")) {
                    log(error);
                } else {
                    try {
                        entries = entries.map(entry => {
                            let fixedEntry = {};

                            for (let key in entry) {
                                let nestedObject = entry[key];

                                if (typeof nestedObject === 'object' && nestedObject !== null) {
                                    let sanitizedNestedObject = {};
                                    for (let nestedKey in nestedObject) {
                                        let sanitizedNestedKey = nestedKey.replace(/\$/g, ''); // Rimuove i `$` dalle chiavi
                                        sanitizedNestedObject[sanitizedNestedKey] = nestedObject[nestedKey]; // Mantiene gli array di valori
                                    }
                                    fixedEntry[key] = sanitizedNestedObject;
                                } else {
                                    fixedEntry[key] = nestedObject;
                                }
                            }

                            return fixedEntry;
                        });

                        await Entries.insertMany(entries);
                    } catch (error) {
                        log("There are problems inserting objects in MongoDB");
                        log(error);
                    }
                }
            }

            let valuesToDB = []

            for (let entry of entries)
                for (let key in entry)
                    for (let subKeyAliasValue in entry[key]) {
                        let existingEntry = valuesToDB.find(v => v.value === subKeyAliasValue)
                        if (existingEntry)
                            existingEntry.visibility = [...new Set([...existingEntry.visibility, ...entry[key][subKeyAliasValue]])]
                        else
                            valuesToDB.push({ value: subKeyAliasValue, visibility: entry[key][subKeyAliasValue] })
                    }

            let keysToDB = entries.map(obj => ({
                key: Object.keys(obj).pop() || "flag_error_key_missing",
                visibility: obj[Object.keys(obj).pop()][Object.keys(obj[Object.keys(obj).pop()]).pop()],

            })
            )
            await Key.insertMany(keysToDB)
            await Value.insertMany(valuesToDB)

            syncing = false
            logger.info("Syncing finished")
            console.info("Syncing finished")
            return "Sync finished"
        }
        else {
            logger.info("Syncing not finished")
            return "Syncing"
        }
    }
    catch (error) {
        logger.error(error)
    }
}

if (!config.doNotSyncAtStart)
    sync()
if (config.syncInterval)
    setInterval(sync, config.syncInterval);

function bucketIs(record, bucket) {
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

    async getKeys(prefix, bucketName, visibility, search) {
        if (visibility == "private")
            visibility = prefix.split("/")[0]
        else if (visibility == "shared")
            visibility = bucketName.toUpperCase() + " SHARED Data"
        else
            visibility = "public-data"
        console.debug(visibility)
        let keys = await Key.find({
            key: { $regex: "^" + search, $options: "i" },
            visibility
        }, { "key": 1, "_id": 0 })//, { "key": 1, "value": 1, "_id": 0, "visibility":0 })
        if (keys.lenght > 500)
            return ["Too many suggestions. Type some characters in order to reduce them"]
        return keys
    },

    async getValues(prefix, bucketName, visibility, search) {
        if (visibility == "private")
            visibility = prefix.split("/")[0]
        else if (visibility == "shared")
            visibility = bucketName.toUpperCase() + " SHARED Data"
        else
            visibility = "public-data"
        console.debug(visibility)
        let values = await Value.find({
            value: { $regex: "^" + search, $options: "i" },
            visibility
        }, { "value": 1, "_id": 0 })//, { "key": 1, "value": 1, "_id": 0, "visibility":0 })
        if (values.lenght > 500)
            return ["Too many suggestions. Type some characters in order to reduce them"]
        return values
    },

    async getEntries(prefix, bucketName, visibility, searchKey, searchValue) {
        //return await Entries.find()
        if (visibility == "private")
            visibility = prefix.split("/")[0]
        else if (visibility == "shared")
            visibility = bucketName.toUpperCase() + " SHARED Data"
        else
            visibility = "public-data"
        console.debug(visibility)
        let entries = await Entries.find({
            "key": { $regex: "^" + searchKey, $options: "i" },
            "value": { $regex: "^" + searchValue, $options: "i" },
            visibility
        }, { "key": 1, "value": 1, "_id": 0 })
        //if (entries.lenght > 500)
        //    return ["Too many suggestions. Type some characters in order to reduce them"]
        return entries
    },


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

        logger.debug("example query geojson: query ", query)

        //let key
        //for (let k in query)
        //    key = k
        let found = []
        let propertiesQuery = {}
        //TODO now there is a preset deep level search, but this level should be parametrized

        for (let key in query)
            if (key != "coordinates")
                propertiesQuery[`properties.${key}`] = query[key]

        if (query.coordinates)
            found.push(
                ...(await Source.find({
                    "features": {
                        $elemMatch: {
                            //"properties.query.key": {
                            ...propertiesQuery,
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
                            ...propertiesQuery,
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
                            ...propertiesQuery,
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
                            ...propertiesQuery,
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
                            ...propertiesQuery,
                            "geometry.coordinates": {
                                $elemMatch: {
                                    //$gte: Number(query.coordinates) - 0.0000001,
                                    //$lte: Number(query.coordinates) + 0.0000001
                                    $eq: Number(query.coordinates) //$elemMatch: { $eq: Number(query[key]) }
                                }
                            }
                        }
                    }
                }))
            )

        else
            found = await Source.find({
                "features": {
                    $elemMatch: {
                        ...propertiesQuery
                    }
                }
            })
        //logger.debug("Found : ", found.length, " items")
        //logger.debug(found)
        //logger.debug({ ...propertiesQuery })
        return found
    },

    async simpleQuery(query) {
        let result = await Source.find(query)
        for (let obj of result) {
            obj.fileName = obj.name.split("/")[obj.name.split("/").lenght - 2]
            obj.path = obj.name
            obj.fileType = obj.name.split(".")[obj.name.split(".").length - 1]
        }
        logger.info(result)
        return result
    },

    async mongoQuery(query, prefix, bucket, visibility) {
        logger.debug("format ", query.format)
        let format = query.format?.toLowerCase()
        if (format)
            delete query["format"]
        logger.debug("format ", format)
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
        logger.info("Raw query")
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
                logger.error(error)
            }
        }
        return objects.filter(obj => typeof obj.raw == "string" ? objectFilter(obj, prefix, bucket, visibility) && (!query.value || obj.raw.includes(query.value)) : objectFilter(obj, prefix, bucket, visibility) && (!query.value || JSON.stringify(obj.raw).includes(query.value)))
        //return objects.filter(obj => typeof obj.raw == "string" ? obj.record.name.includes(prefix) && obj.raw.includes(query.value) : obj.record.name.includes(prefix) && JSON.stringify(obj.raw).includes(query.value))
    },


    querySQL(response, query, prefix, bucket, visibility) {
        client.query(query, (err, res) => {
            if (err) {
                logger.error("ERROR");
                logger.error(err);
                response.status(500).json(err.toString())
                logger.info("Query sql finished with errors")
                return;
            }
            else {
                response.send(res.rows.filter(obj => objectFilter(obj, prefix, bucket, visibility)).map(obj => obj.element && obj.name.split(".").pop() == "csv" ? { ...obj, element: json2csv(obj.element) } : obj))
                logger.info(res.rows);
                logger.info("Query sql finished")
            }
        });
    }
}