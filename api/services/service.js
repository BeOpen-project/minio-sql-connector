const logger = require('percocologger')
const log = logger.info
const Source = require('../models/Source')
const Value = require('../models/Value')
const Key = require('../models/Key')
const Entries = require('../models/Entries')
const Datapoints = require("../models/Datapoint")
const { json2csv } = require('../../utils/common')
const config = require('../../config')
const minioWriter = require("../../inputConnectors/minioConnector")
const axios = require('axios')
const { updateJWT } = require('../../utils/keycloak')
let bearerToken
updateJWT().then(token => {
    bearerToken = token
    logger.info("Initial Keycloak token obtained")
}).catch(error => logger.error(error.response?.data || error));

const path = require('path');
let attrWithUrl = config.orion?.attrWithUrl || "datasetUrl"
require('../../inputConnectors/apiConnector')
//const apiConnector = require('../../inputConnectors/apiConnector')

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

    notifyPath: async (req, res) => {

        logger.info({ body: JSON.stringify(req.body) })

        const data = req.body.data || req.body.value || req.body;
        const entities = Array.isArray(data) ? data : [data];

        for (const ent of entities) {
            const id = ent.id || ent['@id'] || 'unknown-id';
            let urlValue;
            if (ent[attrWithUrl] && typeof ent[attrWithUrl] === 'object' && 'value' in ent[attrWithUrl]) {
                urlValue = ent[attrWithUrl].value;
            } else if (ent[attrWithUrl]) {
                urlValue = ent[attrWithUrl];
            } else if (ent[attrWithUrl + ':value']) {
                urlValue = ent[attrWithUrl + ':value'];
            } else if (ent.value) {
                urlValue = ent.value;
            }

            if (!urlValue || typeof urlValue !== 'string') {
                console.warn(`no URL found for entity ${id}`);
                continue;
            }

            let mapID = req.query.mapID || req.params.mapID || ent.mapID || config.mapID

            if (!mapID) {
                const response = await axios.get(urlValue);
                if (response?.data?.data?.datapoints)
                    await Datapoints.insertMany(response.data.data.datapoints)
                else
                    await minioWriter.insertInDBs(response.data, {
                        name: id + '-' + path.basename((new URL(urlValue)).pathname),
                        lastModified: new Date(),
                        versionId: 'null',
                        isDeleteMarker: false,
                        bucketName: 'orion-notify',
                        size: response.data.length,
                        isLatest: true,
                        etag: '',
                        insertedBy: 'orion-notify'
                    });
            }
            else {
                let response
                let retry = 2
                while (retry > 0)
                    try {
                        response = await axios.post(
                            config.mapEndpoint,
                            {
                                "sourceDataType": "json",
                                "sourceDataURL": urlValue,
                                "decodeOptions": {
                                    "decodeFrom": "json-stat"
                                },
                                "config": {
                                    "NGSI_entity": false,
                                    "ignoreValidation": true,
                                    "writers": [],
                                    "disableAjv": true,
                                    "mappingReport": true
                                },
                                "dataModel": {
                                    "$schema": "http://json-schema.org/schema#",
                                    "$id": "dataModels/DataModelTemp.json",
                                    "title": "DataModelTemp",
                                    "description": "Bike Hire Docking Station",
                                    "type": "object",
                                    "properties": {
                                        "region": {
                                            "type": "string"
                                        },
                                        "source": {
                                            "type": "string"
                                        },
                                        "timestamp": {
                                            "type": "string"
                                        },
                                        "survey": {
                                            "type": "string"
                                        },
                                        "dimensions": {
                                            "type": "object"
                                        },
                                        "value": {
                                            "type": "integer"
                                        }
                                    }
                                }
                            }/*
                        {
                            //mapID,
                            sourceDataURL: urlValue
                        }*/,
                            {
                                headers: {
                                    "Authorization": `Bearer ${bearerToken}`
                                }
                            });
                        retry -= 2
                    }
                    catch (error) {
                        logger.error("Error fetching mapped data from API Connector:", error.response?.data || error.message);
                        bearerToken = await updateJWT(true);
                        retry--
                    }
                //logger.info(response.data.lenght)

                await Datapoints.insertMany(response.data)//.map(d => {return {...d, dimensions : {...(d.dimensions), year : d.dimensions.time}}})) //TODO check if datapoints or other data and generalize insertion
                /*for (let i in response.data)
                    await minioWriter.insertInDBs(response.data[i], {
                        name: response.data[i].id || mapID + '-' + path.basename((new URL(urlValue)).pathname) + i,
                        lastModified: new Date(),
                        versionId: 'null',
                        isDeleteMarker: false,
                        bucketName: 'orion-notify',
                        size: response.data.length,
                        isLatest: true,
                        etag: '',
                        insertedBy: 'orion-notify'
                    });*/
            }
            logger.info(`downloaded ${urlValue}`);
        }
        return 'OK';
    },

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
        }, { "key": 1, "_id": 0 })
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
        }, { "value": 1, "_id": 0 })
        if (values.lenght > 500)
            return ["Too many suggestions. Type some characters in order to reduce them"]
        return values
    },

    async updateOwner(bearer, email) {
        let sources = await Source.find({ name: { $regex: email, $options: 'i' } });
        let sourcesDetails = (await axios.get(config.minioConfig.ownerInfoEndpoint + "/user/listFiles?email=" + email,
            {
                headers: {
                    Authorization: bearer
                }
            })).data
        for (let source of sources) {
            let owner
            let ownerEmail = sourcesDetails.find(obj => obj.objectPath == source.record.name)?.insertedBy
            if (ownerEmail) {
                source.record.insertedBy = ownerEmail
                await Source.updateOne({ _id: source._id }, { $set: { "record.insertedBy": ownerEmail } })
            }
            else
                try {
                    owner = (await axios.get(config.minioConfig.ownerInfoEndpoint + "/createdBy?filePath=" + source.record.name + "&etag=" + source.record.etag,
                        {
                            headers: {
                                Authorization: bearer
                            }
                        })).data
                    source.record.insertedBy = owner
                    await source.save()
                }
                catch (error) {
                    logger.error(error.toString())
                }
        }
        process.queryEngine.updatedOwners[email] = true
    },

    async getEntries(prefix, bucketName, visibility, searchKey, searchValue) {
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

        return entries
    },


    async exampleQueryCSV(query) {

        return await Source.find({
            "csv": {
                $elemMatch: query
            }
        })
    },

    sync: minioWriter.sync,

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
                            ...propertiesQuery,
                            "geometry.coordinates": {
                                $elemMatch: {
                                    $elemMatch: {
                                        $elemMatch: {
                                            $elemMatch: {
                                                $eq: Number(query.coordinates)
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
                                                $eq: Number(query.coordinates)
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
                                            $eq: Number(query.coordinates)
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
                                        $eq: Number(query.coordinates)
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

                                    $eq: Number(query.coordinates)
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
        switch (format) {
            case "geojson": return (await this.exampleQueryGeoJson(query)).filter(obj => objectFilter(obj, prefix, bucket, visibility))
            case "csv": return (await this.exampleQueryCSV(query)).filter(obj => objectFilter(obj, prefix, bucket, visibility))
            case "json": return (await this.exampleQueryJson(query)).filter(obj => objectFilter(obj, prefix, bucket, visibility))
            case "object": return (await this.simpleQuery(query)).filter(obj => objectFilter(obj, prefix, bucket, visibility))
            default: return (await this.simpleQuery(query)).filter(obj => objectFilter(obj, prefix, bucket, visibility))
        }
    },

    async rawQuery(query, prefix, bucket, visibility) {
        logger.info("Raw query")
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