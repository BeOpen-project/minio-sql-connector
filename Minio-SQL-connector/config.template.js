module.exports = {
  minioConfig: {
    endPoint: 'play.min.io',
    port: 9000,
    useSSL: true,
    accessKey: 'Q3AM3UQ867SPQQA43P2F',
    secretKey: 'zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG',
    location: "us-east-1",
    defaultFileInput: "../../input/inputFile.json",
    defaultOutputFolderName: "private generic data",
    defaultInputFolderName: "data model mapper",
    subscribe: {
      all: true,
      buckets: []
    }
  },
  postgreConfig: {
    user: '',
    host: 'localhost',
    database: '',
    password: '',
    port: 5432
  },
  syncInterval :  86400000,
  mongo: "mongodb://localhost:27017/Minio-Mongo", // mongo url
  authConfig: {
    idmHost: "https://platform.beopen-dep.it/auth",
    clientId: "",
    userInfoEndpoint: "https://platform.beopen-dep.it/api/user",//"http://localhost:5500/api/mockGetUser",
    //disableAuth: true,
    authProfile: "oidc",
    authRealm: "",
    introspect: false,
    publicKey: "",
    secret: "" // don't push it
},
}