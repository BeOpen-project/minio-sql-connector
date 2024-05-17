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

  getNotifications(bucketName) {
    let csv = false
    const poller = minioClient.listenBucketNotification(bucketName, '', '', ['s3:ObjectCreated:*'])
    poller.on('notification', async (record) => {
      console.info('New object: %s/%s (size: %d)', record.s3.bucket.name, record.s3.object.key, record.s3.object.size)
      const newObject = await this.getObject(record.s3.bucket.name, record.s3.object.key)
      //console.log(record.s3)
      //console.log(record)
      let jsonParsed, jsonStringified
      try {
        jsonParsed = JSON.parse(newObject)
        console.log("E' un json")
        //console.log(newObject, "\n", typeof newObject)
      }
      catch (error) {
        console.error("Not a json")
        console.error(error.toString())
        jsonStringified = common.convertCSVtoJSON(newObject)
        //console.log(jsonStringified, "\n", typeof jsonStringified)
        csv = true
        //jsonParsed = newObject
      }

      let table = record.s3.bucket.name

      this.client.query("SELECT * FROM " + table + " WHERE name = '" + record.s3.object.key + "'", (err, res) => {
        if (err) {
          console.error("ERROR searching object in DB");
          console.error(err);
          return;
        }
        console.log("Objects found \n ", res.rows);
        if (res.rows[0])
          this.client.query(`UPDATE ${table} SET data = '${jsonStringified || common.cleaned(newObject)}' WHERE name = '${record.s3.object.key}'`, (err, res) => {
            if (err) {
              console.error("ERROR updating object in DB");
              console.error(err);
              return;
            }
            //console.log("Object updated \n", res.rows);
            console.log("Object updated \n");

          });
        else
          this.client.query(`INSERT INTO ${table} (name, data) VALUES ('${record.s3.object.key}', '${jsonStringified || common.cleaned(newObject)}')`, (err, res) => {
            if (err) {
              console.error("ERROR inserting object in DB");
              console.error(err);
              return;
            }
            console.log("Object inserted \n");

            //console.log("Object inserted \n", res.rows);
          });

      });

      jsonParsed = JSON.parse(jsonStringified || newObject)
      jsonParsed.record = record
      try {
        await Source.deleteOne({ 'record.s3.object.key': record.s3.object.key })//record.s3.object
      }
      catch (error) {
        console.error(error)
      }

      //---
      //let foundObject = (await Source.find({ 'record.s3.object.key' : record.s3.object.key }))[0]
      //if (foundObject)
      //  await Source.findOneAndReplace({ 'record.s3.object.key': record.s3.object.key }, jsonParsed
      //format ? 
      /*
      {
        name: record.s3.object.key,
        source: jsonParsed ? jsonParsed : newObject,
        bucket: record.s3.bucket.name,
        timestamp: Date.now()
      }*/
      //: { name: record.s3.object.key, sourceCSV: newObject, bucket: record.s3.bucket.name, from: "minio", timestamp: new Number(Date.now()) }
      //)
      //else
      //---
      try {
        await Source.insertMany([csv ? { csv: jsonParsed } : Array.isArray(jsonParsed) ? {json : jsonParsed} : jsonParsed//record.s3.object
          //format ? 
          /*{
            name: record.s3.object.key,
            source: jsonParsed ? jsonParsed : newObject,
            bucket: record.s3.bucket.name,
            timestamp: Date.now()
          }*/
          //: { name: record.s3.object.key, sourceCSV: newObject, bucket: record.s3.bucket.name, from: "minio", timestamp: new Number(Date.now()) }
        ])
      }
      catch (error) {
        console.error(error)
      }
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
          resultMessage = format == 'json' ? JSON.parse(objectData) : objectData
        }
        catch (error) {
          console.error(error)
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