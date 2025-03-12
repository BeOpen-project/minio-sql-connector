const Log = require('../../utils/logger')//.app(module);
const { Logger } = Log
const logger = new Logger("service")
const log = logger.info
const Source = require('../models/source')
const Value = require('../models/value')
const Key = require('../models/key')
const Entries = require('../models/entries')
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
            logger.debug("Subscribed bucket " + (a++) + " of " + buckets.length, "(", bucket.name, ")")
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
        await Source.deleteMany({})
        await Key.deleteMany({})
        await Value.deleteMany({})
        await Entries.deleteMany({})
        //await sleep(10000)
        let objects = []
        let buckets = await minioWriter.listBuckets()
        let bucketIndex = 1
        for (let bucket of buckets) {
            let bucketObjects = await minioWriter.listObjects(bucket.name)
            let index = 1
            for (let obj of bucketObjects) {
                try {
                    await sleep(delays)
                    logger.debug("Bucket ", bucketIndex, " of ", buckets.length)
                    logger.debug("Scanning object ", index++, " of ", bucketObjects.length, ",", obj.name)
                    let extension = obj.name.split(".").pop()
                    let isAllowed = (queryAllowedExtensions == "all" || queryAllowedExtensions.includes(extension))
                    if (obj.size && obj.isLatest && isAllowed) {//} && !obj.isDeleteMarker) {
                        let objectGot = await minioWriter.getObject(bucket.name, obj.name, obj.name.split(".").pop())
                        objects.push({ raw: objectGot, info: { ...obj, bucketName: bucket.name } })
                    }
                    else logger.log("Size is ", obj.size, ", ", (obj.isLatest ? "is latest" : "is not latest"), " and extension ", (isAllowed ? "is allowed" : "is not allowed"))
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
            await minioWriter.insertInDBs(obj.raw, obj.info, true)

        //minioWriter.entities.values = minioWriter.entities.values.map(obj => ({ ...obj, visibility: getVisibility(obj) }))
        //minioWriter.entities.keys = minioWriter.entities.keys.map(obj => ({ ...obj, visibility: getVisibility(obj) }))
        //minioWriter.entities.entries = minioWriter.entities.entries.map(obj => ({ ...obj, visibility: getVisibility(obj) }))

        const existingValues = (await Value.find({ value: { $in: minioWriter.entities.values.map(v => v.value) } }))//.map(v => v.value);
        const existingKeys = (await Key.find({ key: { $in: minioWriter.entities.keys.map(k => k.key) } }))//.map(k => k.key); //{ key: 1, _id: 0 }
        const existingEntries = await Entries.find({ key: { $in: minioWriter.entities.entries.map(e => e.key) } });

        // inizio mod
        const existingValuesMap = new Map();
        existingValues.forEach(v => {
            if (!existingValuesMap.has(v.name)) {
                existingValuesMap.set(v.name, new Set());
            }
            existingValuesMap.get(v.name).add(v.value);
        });
        const uniqueValues = existingValues[0] ? minioWriter.entities.values.filter(entry => !existingValuesMap.has(entry.name) || !existingValuesMap.get(entry.name).has(entry.value)) : minioWriter.entities.values;
        // fine mod

        //const existingValuesSet = new Set(existingValues);
        //const uniqueValues = existingValues[0] ? minioWriter.entities.values.filter(value => !existingValuesSet.has(value.value)) : minioWriter.entities.values;
        try {
            if (uniqueValues.length > 0) await Value.insertMany(uniqueValues);
        }
        catch (error) {
            if (!error?.errorResponse?.message?.includes("Document can't have"))
                log(error)
            else
                try {
                    await Value.insertMany(uniqueValues.map(v => ({ value: v.value.replace(/\$/g, '') })))
                }
                catch (error) {
                    log("There are problems inserting object in mongo DB")
                    log(error)
                }
        }

        // inizio mod
        const existingKeysMap = new Map();
        existingKeys.forEach(k => {
            if (!existingKeysMap.has(k.name)) {
                existingKeysMap.set(k.name, new Set());
            }
            existingKeysMap.get(k.name).add(k.key);
        });
        const uniqueKeys = existingKeys[0] ? minioWriter.entities.keys.filter(entry => !existingKeysMap.has(entry.name) || !existingKeysMap.get(entry.name).has(entry.key)) : minioWriter.entities.keys;
        // fine mod
        //const existingKeysSet = new Set(existingKeys);
        //const uniqueKeys = existingKeys[0] ? minioWriter.entities.keys.filter(key => !existingKeysSet.has(key.key)) : minioWriter.entities.keys;
        try {
            if (uniqueKeys.length > 0) await Key.insertMany(uniqueKeys);
            /*else {
                logger.error(uniqueKeys, minioWriter.entities.keys)
                process.exit(1)
            }*/
        }
        catch (error) {
            if (!error?.errorResponse?.message?.includes("Document can't have"))
                log(error)
            else
                try {
                    await Key.insertMany(uniqueKeys.map(k => ({ key: k.key.replace(/\$/g, '') })))
                }
                catch (error) {
                    log("There are problems inserting object in mongo DB")
                    log(error)
                }
        }

        /*
        const existingEntriesMap = new Map();
        existingEntries.forEach(e => {
            if (!existingEntriesMap.has(e.key)) {
                existingEntriesMap.set(e.key, new Set());
            }
            existingEntriesMap.get(e.key).add(e.value);
            existingEntriesMap.get(e.key).add(e.name);
        });
        const uniqueEntries = existingEntries[0] ? minioWriter.entities.entries.filter(entry => !existingEntriesMap.has(entry.key) || (!existingEntriesMap.get(entry.key).has(entry.value)) && !existingEntriesMap.get(entry.key).has(entry.name)) : minioWriter.entities.entries;*/

        //mod2
        // Creiamo una mappa che tiene traccia dei valori di 'value' e 'name' separatamente per ogni 'key'
        const existingEntriesMap = new Map();
        existingEntries.forEach(e => {
            if (!existingEntriesMap.has(e.key)) {
                existingEntriesMap.set(e.key, { values: new Set(), names: new Set() });
            }
            existingEntriesMap.get(e.key).values.add(e.value);
            existingEntriesMap.get(e.key).names.add(e.name);
        });

        // Filtra le voci uniche
        const uniqueEntries = existingEntries[0]
            ? minioWriter.entities.entries.filter(entry => {
                const entryMap = existingEntriesMap.get(entry.key);
                return (
                    !entryMap ||  // Se non esiste una voce con quella 'key', è unica
                    (!entryMap.values.has(entry.value) && !entryMap.names.has(entry.name)) // Se il valore o il nome non esistono, è unica
                );
            })
            : minioWriter.entities.entries;
        //finemod2
        try {
            if (uniqueEntries.length > 0) await Entries.insertMany(uniqueEntries);
            /*else {
                logger.error(uniqueEntries, minioWriter.entities)
            }*/
        }
        catch (error) {
            if (!error?.errorResponse?.message?.includes("Document can't have"))
                log(error)
            else
                try {
                    await Entries.insertMany(uniqueEntries.map(e => ({ key: e.key.replace(/\$/g, ''), value: e.value.replace(/\$/g, '') })))
                }
                catch (error) {
                    log("There are problems inserting object in mongo DB")
                    log(error)
                }
        }
        syncing = false
        logger.log("Syncing finished")
        return "Sync finished"
    }
    else {
        logger.log("Syncing not finished")
        return "Syncing"
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

    async getKeys() {
        return await Key.find()
    },

    async getValues() {
        return await Value.find()
    },

    async getEntries() {
        return await Entries.find()
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
        logger.log(result)
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
        logger.log("Raw query")
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
                logger.log("Query sql finished with errors")
                return;
            }
            else {
                response.send(res.rows.filter(obj => objectFilter(obj, prefix, bucket, visibility)).map(obj => obj.element && obj.name.split(".").pop() == "csv" ? { ...obj, element: json2csv(obj.element) } : obj))
                logger.log(res.rows);
                logger.log("Query sql finished")
            }
        });
    }
}