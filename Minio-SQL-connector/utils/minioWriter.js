var Minio = require('minio')
const { sleep } = require('./common.js')
const common = require('./common.js')
const { minioConfig } = require('../config.js')
const Source = require('../api/models/source.js')

let minioClient = new Minio.Client(minioConfig)
let logs = []
const fs = require('fs');
const logFile = 'log.txt';
const logStream = fs.createWriteStream(logFile, { flags: 'a' });

function logSizeChecker() {
  let stats = fs.statSync(logFile)
  let fileSizeInBytes = stats.size;
  // Convert the file size to megabytes (optional)
  let fileSizeInMegabytes = fileSizeInBytes / (1024 * 1024);
  log("Log size is ", fileSizeInMegabytes)
  if (fileSizeInMegabytes > 50)
    fs.writeFile(logFile, "", err => {
      if (err) {
        console.error(err);
      } else {
        log("Log reset")
      }
    });
}

//setInterval(logSizeChecker, 3600000);

function log(...m) {
  console.log(...m)
  //logs.push(m)

  /*
  let args = [...m]
  for (let arg of args)
    if (arg && (Array.isArray(arg) || typeof arg == "object"))
      logStream.write(JSON.stringify(arg) + '\n');
    else
      logStream.write(arg + '\n');
    */
}

module.exports = {

  client: undefined,

  subscribe(bucket) {
    minioClient.getBucketNotification(bucket, function (err, bucketNotificationConfig) {
      if (err) return log(err)
      log("SUBSCRIBE", bucketNotificationConfig)
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
      if (err) return log(err)
      log('Success')
    })
  },

  async listObjects(bucketName, prefix, recursive) {


    let resultMessage
    let errorMessage

    var data = []
    var stream = minioClient.listObjects(bucketName, '', true, { IncludeVersion: true })
    //var stream = minioClient.extensions.listObjectsV2WithMetadata(bucketName, '', true, '')
    stream.on('data', function (obj) {
      log(bucketName)
      //log(common.minify(obj))
      data.push(obj)
    })
    stream.on('end', function (obj) {
      if (!obj)
        log("ListObjects ended returning an empty object")
      else
        log("Found object ")// + common.minify(JSON.stringify(obj)))
      if (data[0])
        //log(common.minify(JSON.stringify(data)))
      resultMessage = data
      //process.res.send(data)
    })
    stream.on('error', function (err) {
      log(err)
      errorMessage = err
    })

    let logCounterFlag
    while (!errorMessage && !resultMessage) {
      await sleep(1)
      if (!logCounterFlag) {
        logCounterFlag = true
        sleep(1000).then(resolve => {
          if (!errorMessage && !resultMessage)
            log("waiting for list")
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
        log("New object\n")//, common.minify(newObject), "\ntype : ", typeof newObject)
        jsonParsed = JSON.parse(newObject)
        log("Adesso è un json")
      }
      catch (error) {
        log("Not a json")
        //log(error)
        let extension = (record?.s3?.object?.key || record.name).split(".").pop()
        if (extension == "csv")
          jsonStringified = common.convertCSVtoJSON(newObject)
        csv = true
      }
    else {
      log("Era già un json")
      jsonParsed = newObject
    }

    let table = common.urlEncode(record?.s3?.bucket?.name || record.bucketName)
    //log(record)

    //log("before postgre query", common.minify(record?.s3?.object))
    //log("before postgre query", common.minify(JSON.stringify(jsonStringified || common.cleaned(newObject))))
    let queryName = record?.s3?.object?.key || record.name
    //log("QUERY NAME", queryName)
    //queryName.replace(/ /g, '');

    this.client.query("SELECT * FROM " + table + " WHERE name = '" + queryName + "'", (err, res) => {
      if (err) {
        log("ERROR searching object in DB");
        log(err);
        // CREATE TABLE nome-bucket (id SERIAL PRIMARY KEY, name TEXT NOT NULL, data JSONB)
        this.client.query("CREATE TABLE  " + table + " (id SERIAL PRIMARY KEY, name TEXT NOT NULL, data JSONB, record JSONB)", (err, res) => {
          if (err) {
            log("ERROR creating table");
            log(err);
            return;
          }
          //log(common.minify(res))
          this.client.query(`INSERT INTO ${table} (name, data, record) VALUES ('${record?.s3?.object?.key || record.name}', '${JSON.stringify(jsonStringified || common.cleaned(newObject))}', '${JSON.stringify(record)}')`, (err, res) => {
            if (err) {
              log("ERROR inserting object in DB");
              log(err);
              return;
            }
            log("Object inserted \n");
          });

        });
        return;
      }
      log("Objects found \n ")//, common.minify(res.rows));
      if (res.rows[0])
        this.client.query(`UPDATE ${table} SET data = '${JSON.stringify(jsonStringified || common.cleaned(newObject))}' WHERE name = '${record?.s3?.object?.key || record.name}'`, (err, res) => {
          if (err) {
            log("ERROR updating object in DB");
            log(err);
            return;
          }
          log("Object updated \n");

        });
      else
        this.client.query(`INSERT INTO ${table} (name, data) VALUES ('${record?.s3?.object?.key || record.name}', '${JSON.stringify(jsonStringified || common.cleaned(newObject))}')`, (err, res) => {
          if (err) {
            log("ERROR inserting object in DB");
            log(err);
            return;
          }
          log("Object inserted \n");
        });

    });

    //jsonParsed = JSON.parse(JSON.stringify(jsonStringified || newObject))
    if ((!jsonParsed) || (jsonParsed && typeof jsonParsed != "object"))
      try {
        jsonParsed = JSON.parse(jsonStringified || newObject)
      }
      catch (error) {
        log(error)
      }

    try {
      log("DELETE QUERY")
      log({ 'name': (record?.s3?.object?.key || record.name) })

      await Source.deleteMany({ 'name': (record?.s3?.object?.key || record.name) })//record.s3.object
    }
    catch (error) {
      log(error)
    }

    //log(record)

    let name = record?.s3?.object?.key || record.name
    name = name.split(".")
    let extension = name.pop()
    log("Extension ", extension)
    log("E' un array : ", Array.isArray(jsonParsed))
    log("Type ", typeof jsonParsed)
    //log("Il file è questo \n", common.minify(jsonParsed))
    //log("Ecco i dettagli \n", record)
    if (!jsonParsed)
      log("Empty object of extension ", extension)

    try {
      await Source.insertMany([extension == "csv" ? { csv: jsonParsed, record, name: record?.s3?.object?.key || record.name } : Array.isArray(jsonParsed) ? { json: jsonParsed, record, name: record?.s3?.object?.key || record.name } : typeof jsonParsed == "object" ? { ...jsonParsed, record, name: record?.s3?.object?.key || record.name } : { raw: jsonParsed, record, name: record?.s3?.object?.key || record.name }])
    }
    catch (error) {
      //log(error)
      try {
        await Source.insertMany(JSON.parse(JSON.stringify([extension == "csv" ? { csv: jsonParsed, record, name: record?.s3?.object?.key || record.name } : Array.isArray(jsonParsed) ? { json: jsonParsed, record, name: record?.s3?.object?.key || record.name } : typeof jsonParsed == "object" ? { ...jsonParsed, record, name: record?.s3?.object?.key || record.name } : { raw: jsonParsed, record, name: record?.s3?.object?.key || record.name }]).replace(/\$/g, '')))
      }
      catch (error) {
        log(error)
      }
    }
  },

  getNotifications(bucketName) {

    const poller = minioClient.listenBucketNotification(bucketName, '', '', ['s3:ObjectCreated:*'])
    poller.on('notification', async (record) => {
      log('New object: %s/%s (size: %d)', record.s3.bucket.name, record.s3.object.key, record.s3.object.size)
      let newObject = await this.getObject(record.s3.bucket.name, record.s3.object.key, record.s3.object.key.split(".").pop())
      try {
        log("Getting object")
        newObject = await this.getObject(record.s3.bucket.name, record.s3.object.key, record.s3.object.key.split(".").pop())
        log("Got")
      }
      catch (error) {
        log("Error during getting object")
        console.error(error)
        return
      }
      log("New object\n", common.minify(newObject), "\ntype : ", typeof newObject)
      //log(record.s3)
      //log(record)
      await this.insertInDBs(newObject, record, false)
    })
    poller.on('error', (error) => {
      log(error)
      //log("Creating bucket")
      //this.creteBucket(bucketName, minioConfig.location).then(message => {
      //  log(message)
      //  this.getNotifications(bucketName)
      //}
      //)
    })
  },

  async listBuckets() {
    return await minioClient.listBuckets()
  },

  async getObject(bucketName, objectName, format) {

    log("Now getting object " + objectName + " in bucket " + bucketName)

    let resultMessage
    let errorMessage

    minioClient.getObject(bucketName, objectName, function (err, dataStream) {
      if (err) {
        errorMessage = err
        log(err)
        return err
      }

      let objectData = '';
      dataStream.on('data', function (chunk) {
        objectData += chunk;
      });

      dataStream.on('end', function () {
        log('Object data: ')//, common.minify(objectData));
        try {
          //resultMessage = format == 'json' ? JSON.parse(JSON.stringify(objectData)) : objectData
          resultMessage = (format == 'json' && typeof objectData == "string") ? JSON.parse(objectData) : objectData
          log("Json parsato")

        }
        catch (error) {
          //log("Non era un json ? \n", error)
          try {
            //resultMessage = format == 'json' ? JSON.parse(JSON.stringify(objectData)) : objectData
            resultMessage = (format == 'json' && typeof objectData == "string") ? JSON.parse(objectData.substring(1)) : objectData
            log("Json parsato")

          }
          catch (error) {
            log("Non era un json ? \n", error)
            resultMessage = format == 'json' ? [{ data: objectData }] : objectData
          }
        }
        if (!resultMessage)
          resultMessage = "Empty file"
      });

      dataStream.on('error', function (err) {
        log('Error reading object:')
        errorMessage = err
        log(err)
      });

    });

    let logCounterFlag
    while (!errorMessage && !resultMessage) {
      await sleep(100)
      if (!logCounterFlag) {
        logCounterFlag = true
        sleep(1000).then(resolve => {
          if (!errorMessage && !resultMessage)
            log("waiting for object " + objectName + " in bucket " + bucketName)
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