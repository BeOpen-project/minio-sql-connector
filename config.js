module.exports = {
    minioConfig: {
        endPoint: 'localhost',//'platform.beopen-dep.it/minio',
        //endPoint2: 'platform.beopen-dep.it/minio',//'platform.beopen-dep.it/minio',
        //endPoint: 'kubernetes.docker.internal',//'platform.beopen-dep.it/minio',
        //endPoint: 'play.min.io',
        port: 9000,//5502,
        useSSL: false,
        //windows
        //accessKey: "hB4EPXbcrcjj9keq5dkV",
        //windows
        //secretKey: "507nlV3QyJz64zEcRzdgpnXiKO2DS4XgP1Vzm7Yw",
        //linux 
        accessKey: "admin",
        secretKey: "BeOp3nPassword",
        //accessKey: "2wUene3VMnLTamVnHgno",//"DoWmPwWdLwc3DJbgoUR2",//"0vXPJu5qi8lgjw0ESkRO", //'VsnPS4aRJUgdanZJvGCO',//'Q3AM3UQ867SPQQA43P2F',
        //linux 
        //secretKey: "lW8n4RlacvxzjNeA4zJjbpmstb5PYfkutNkEzoF3",//"kmaD7eJgSxHNgpXddZyNA8Fu2MS0iIsgoLF447cY",//"Z682e347pS5SWfqfIp1Merp80IGvL8IHFt0ilnvf",//'XjkKjbWNgY2g4AlgoQILiHbOThFgMa69MwhNz1JQ',// 'zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG',
        location: "us-east-1",
        defaultFileInput: "../../input/inputFile.json",
        defaultOutputFolderName: "",
        defaultInputFolderName: "",
        //defaultOutputBucketName: "private generic data",
        //defaultBucketName: "data model mapper",
        subscribe: {
            all: true,
            buckets: []// ["sharedbucket"]//["datamodelmapper"],
            //buckets: []
        },
        defaultBucket: "default",
        ownerInfoEndpoint: "", // "https://platform.beopendep.it/api",//"https://platform.beopendep.it/api/owner",
        //location: "eu",
        //accessKey: 'lQrNpaEPjRMLuUVirPfK',
        //secretKey: 'DgurHiJWWYRM83pGDWMoTEZb5tr0E7IEoHwmGelk',
    },
    orion: {
        subscribe: true,
        subscribeType: "DistributionDCAT-AP",
        watchedAttributes: ["downloadURL", "modifiedDate", "modifiedDate.@value", "format"],
        deleteAllDuplicateSubscriptions: true,
        attrWithUrl: "downloadURL",
        orionBaseUrl: "http://localhost:1027",
        notificationUrl: "http://host.docker.internal:3000/api/orion/subscribe/6914a252ddb96948ee67b2e1",
        fiwareService: "smartera",
        fiwareServicePath: "/smartera",
        purgeSubscriptionsAtStart: true,
    },
    postgreConfig: {
        user: 'my_user',
        host: 'localhost',
        database: 'my_database',
        password: 'my_password',
        port: 5432, // Porta di default per PostgreSQL
    },
    mapEndpoint: "http://localhost:5500/api/map/transform",
    mapID: "",
    logLevel: "debug",
    mongo: "mongodb://localhost:22000/Minio-Mongo",//"mongodb://localhost:22000/Minio-Mongo", // mongo url
    syncInterval: 86400000, //86400000 or 0
    doNotSyncAtStart: false,
    writeLogsOnFile: true,
    delays: 1,
    queryAllowedExtensions: ["csv", "json", "geojson"],
    parseCompatibilityMode: 0,
    updateOwner: "later",
    authConfig: {
        idmHost: "http://localhost:8080",
        clientId: "dmm",
        username: "percoco",
        password: "dmm",
        userInfoEndpoint: "",// "https://platform.beopendep.it/api/user",//"http://localhost:5500/api/mockGetUser",
        disableAuth: true,
        authProfile: "oidc",
        authRealm: "master",
        introspect: false,
        publicKey: "-----BEGIN PUBLIC KEY-----\n" +
            "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAvWjvqtmPssFkWaTwSKnSY7ASzOZsgLzTP+Qx1WGq82qcUw3wjYEcCPvN+zxLdTz49E5WI0sYQtVGcVHFVGA31l5mYLsQOA7ppthXQPg2XyVqoNeWreAAw5GwMVB60/daK9fXND9pcD4lmMYo0TNzj6GxsCBsJtLb6rk9j2O40JMN634upRLCwYKxCNUUjjw9iz+8zm9WDjIOz8O0t3x8elxj8ZygulDfogAfUBIWlDLLP9JhrBdB6mTJY/aAnKAHKYF1v119LjbLhApfjy2qOCApVwMpM+cYjcHuT1Nga4mYqOC6EtA9cxYQlb+AHTBeIv9giLPaVGizvqPmox0s7QIDAQAB" +
            "\n-----END PUBLIC KEY-----",
        secret: "" // don't push it
    },
    sourceConnectors : {
        minioConnector : true,
        apiConnector : true
    },
    queryOptions: {
        simpleSearch: true,
        advancedSearch: true,
        SQLQuery: true,
        graphQLQuery: true
    }
}
