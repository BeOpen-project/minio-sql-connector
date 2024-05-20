var Minio = require('minio')
const { sleep } = require('./common')
const common = require('./common')
const { minioConfig } = require('./config')
const Source = require('./source.js')

let minioClient = new Minio.Client(minioConfig)

module.exports = {

  client: undefined,

  subscribe(bucket) {
    minioClient.getBucketNotification(bucket, function (err, bucketNotificationConfig) {
      if (err) return console.error(err)
      console.info(bucketNotificationConfig)
    })
  },

  setNotifications(bucket) {

    var bucketNotification = new Minio.NotificationConfig()
    var arn = Minio.buildARN('aws', 'sqs', 'us-east-1', '1', 'webhook')
    var queue = new Minio.QueueConfig(arn)
    queue.addEvent(Minio.ObjectReducedRedundancyLostObject)
    queue.addEvent(Minio.ObjectCreatedAll)
    bucketNotification.add(queue)
    minioClient.setBucketNotification(bucket, bucketNotification, function (err) {
      if (err) return console.error(err)
      console.info('Success')
    })
  },

  async listObjects(bucketName, prefix, recursive) {


    let resultMessage
    let errorMessage

    var data = []
    var stream = minioClient.listObjects(bucketName, '', true, { IncludeVersion: true })
    //var stream = minioClient.extensions.listObjectsV2WithMetadata(bucketName, '', true, '')
    stream.on('data', function (obj) {
      console.debug(bucketName)
      console.debug(obj)
      data.push(obj)
    })
    stream.on('end', function (obj) {
      if (!obj)
        console.info("ListObjects ended returning an empty object")
      else
        console.info("Found object " + JSON.stringify(obj))
      if (data[0])
        console.info(JSON.stringify(data))
      resultMessage = data
      //process.res.send(data)
    })
    stream.on('error', function (err) {
      console.error(err)
      errorMessage = err
    })

    let logCounterFlag
    while (!errorMessage && !resultMessage) {
      await sleep(1)
      if (!logCounterFlag) {
        logCounterFlag = true
        sleep(1000).then(resolve => {
          if (!errorMessage && !resultMessage)
            console.debug("waiting for list")
          logCounterFlag = false
        })
      }
    }
    if (errorMessage)
      throw errorMessage
    if (resultMessage)
      return resultMessage
  },

  async insertInDBs(newObject, record, align) {
    let csv = false
    let jsonParsed, jsonStringified
    if (typeof newObject != "object")
      try {
        //jsonParsed = JSON.parse(JSON.stringify(newObject))
        console.log(typeof newObject, newObject)
        jsonParsed = JSON.parse(newObject)
        console.log("E' un json")
      }
      catch (error) {
        console.error("Not a json")
        console.error(error.toString())
        jsonStringified = common.convertCSVtoJSON(newObject)
        csv = true
      }
    else
      jsonParsed = newObject

    let table = record?.s3?.bucket?.name || record.bucket

    console.debug(record?.s3?.object)

    this.client.query("SELECT * FROM " + table + " WHERE name = '" + (record?.s3?.object?.key || record.key) + "'", (err, res) => {
      if (err) {
        console.error("ERROR searching object in DB");
        console.error(err);
        return;
      }
      console.log("Objects found \n ", res.rows);
      if (res.rows[0])
        this.client.query(`UPDATE ${table} SET data = '${jsonStringified || common.cleaned(newObject)}' WHERE name = '${record?.s3?.object?.key || record.key}'`, (err, res) => {
          if (err) {
            console.error("ERROR updating object in DB");
            console.error(err);
            return;
          }
          console.log("Object updated \n");

        });
      else
        this.client.query(`INSERT INTO ${table} (name, data) VALUES ('${record?.s3?.object?.key || record.key}', '${jsonStringified || common.cleaned(newObject)}')`, (err, res) => {
          if (err) {
            console.error("ERROR inserting object in DB");
            console.error(err);
            return;
          }
          console.log("Object inserted \n");
        });

    });

    //jsonParsed = JSON.parse(JSON.stringify(jsonStringified || newObject))
    if ((!jsonParsed) || (jsonParsed && typeof jsonParsed != "object"))
      try {
        jsonParsed = JSON.parse(jsonStringified || newObject)
      }
      catch (error) {
        console.error(error)
      }

    try {
      await Source.deleteOne({ 'record.s3.object.key': record?.s3?.object?.key || record.key })//record.s3.object
    }
    catch (error) {
      console.error(error)
    }

    let name = record?.s3?.object?.key || record.key
    name = name.split(".")
    let extension = name.pop()
    console.debug(extension)

    try {
      await Source.insertMany([extension == "csv" ? { csv: jsonParsed, record } : Array.isArray(jsonParsed) ? { json: jsonParsed, record } : { ...jsonParsed, ...record }])
    }
    catch (error) {
      console.error(error)
    }
  },

  getNotifications(bucketName) {

    const poller = minioClient.listenBucketNotification(bucketName, '', '', ['s3:ObjectCreated:*'])
    poller.on('notification', async (record) => {
      console.info('New object: %s/%s (size: %d)', record.s3.bucket.name, record.s3.object.key, record.s3.object.size)
      const newObject = await this.getObject(record.s3.bucket.name, record.s3.object.key, record.s3.object.key.split(".").pop())
      console.log(newObject, typeof newObject)
      //console.log(record.s3)
      //console.log(record)
      await this.insertInDBs(newObject, record)
    })
    poller.on('error', (error) => {
      console.error(error)
      console.debug("Creating bucket")
      this.creteBucket(bucketName, minioConfig.location).then(message => {
        console.debug(message)
        this.getNotifications(bucketName)
      }
      )
    })
  },

  async listBuckets() {
    return await minioClient.listBuckets()
  },

  async getObject(bucketName, objectName, format) {

    console.debug("Now getting object " + objectName + " in bucket " + bucketName)

    let resultMessage
    let errorMessage

    minioClient.getObject(bucketName, objectName, function (err, dataStream) {
      if (err) {
        errorMessage = err
        console.error(err)
        return err
      }

      let objectData = '';
      dataStream.on('data', function (chunk) {
        objectData += chunk;
      });

      dataStream.on('end', function () {
        console.info('Object data: ', objectData);
        try {
          //resultMessage = format == 'json' ? JSON.parse(JSON.stringify(objectData)) : objectData
          resultMessage = format == 'json' ? JSON.parse(objectData) : objectData
          console.log("Json parsato")

        }
        catch (error) {
          console.error("Non era un json ? \n", error)
          resultMessage = format == 'json' ? [{ data: objectData }] : objectData
        }
      });

      dataStream.on('error', function (err) {
        console.info('Error reading object:')
        errorMessage = err
        console.error(err)
      });

    });

    let logCounterFlag
    while (!errorMessage && !resultMessage) {
      await sleep(1)
      if (!logCounterFlag) {
        logCounterFlag = true
        sleep(1000).then(resolve => {
          if (!errorMessage && !resultMessage)
            console.debug("waiting for object " + objectName + " in bucket " + bucketName)
          logCounterFlag = false
        })
      }
    }
    if (errorMessage)
      throw errorMessage
    if (resultMessage)
      return resultMessage
    //})
  }
}