const Source = require('../source')
const common = require('../common')
const minioWriter = require ("../minioWriter")

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

    async mongoQuery(query) {
        let format = query.format?.toLowerCase()
        if (format)
            delete query["format"]
        switch (format) {
            case "geojson": return await this.exampleQueryGeoJson(query)
            case "csv": return await this.exampleQueryCSV(query)
            case "json": return this.exampleQueryJson(query)
            case "object": return await Source.find(query)
        }
    }
}