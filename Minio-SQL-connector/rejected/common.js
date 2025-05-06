   /*
    return {
      minioConfig: {
        endPoint: configIn.minioConfig.endPoint || 'localhost',
        port: Number.isInteger(configIn.minioConfig.port) ? configIn.minioConfig.port : 9000,
        useSSL: configIn.minioConfig.useSSL || false,
        accessKey: configIn.minioConfig.accessKey, //there is no default value for this, it must be set
        secretKey: configIn.minioConfig.secretKey, //there is no default value for this, it must be set
        location: configIn.minioConfig.location || "us-east-1",
        defaultFileInput: configIn.minioConfig.defaultFileInput || "../../input/inputFile.json",
        defaultOutputFolderName: configIn.minioConfig.defaultOutputFolderName || "",
        defaultInputFolderName: configIn.minioConfig.defaultInputFolderName || "",
        subscribe: {
          all: configIn.minioConfig.subscribe.all || true,
          buckets: configIn.minioConfig.subscribe.buckets || []
        },
        ownerInfoEndpoint: configIn.minioConfig.ownerInfoEndpoint || "https://platform.beopendep.it/api",
      },
      postgreConfig: {
        user: configIn.postgreConfig.user || 'my_user',
        host: configIn.postgreConfig.host || 'localhost',
        database: configIn.postgreConfig.database || 'my_database',
        password: configIn.postgreConfig.password || 'my_password',
        port: Number.isInteger(configIn.postgreConfig.port) ?  configIn.postgreConfig.port : 5432,
      },
      logLevel: configIn.logLeval || "info",
      mongo: configIn.mongo || "mongodb://localhost:22000/Minio-Mongo", 
      syncInterval: Number.isInteger(configIn.syncInterval) ?  configIn.syncInterval : 86400000, 
      doNotSyncAtStart: configIn.doNotSyncAtStart, // default is false
      writeLogsOnFile: configIn.writeLogsOnFile || true,
      delays: Number.isInteger(configIn.delays) ? configIn.delays : 1,
      queryAllowedExtensions: configIn.queryAllowedExtensions || ["csv", "json", "geojson"],
      parseCompatibilityMode: Number.isInteger(configIn.parseCompatibilityMode)? configIn.parseCompatibilityMode : 0,
      updateOwner: configIn.updateOwner || "later",
      authConfig: {
        idmHost: configIn.authConfig.idmHost || "https://platform.beopendep.it/auth",
        clientId: configIn.authConfig.clientId || "beopen-dashboard",
        userInfoEndpoint: configIn.authConfig.userInfoEndpoint || "https://platform.beopendep.it/api/user",
        authProfile: configIn.authConfig.authProfile || "oidc",
        authRealm: configIn.authConfig.authRealm || "master",
        introspect: configIn.authConfig.introspect, // default is false
        publicKey: configIn.authConfig.publicKey, // there is no default value for this, it must be set
        secret:  configIn.authConfig.secret // there is no default value for this, it must be set
      }
    }*/