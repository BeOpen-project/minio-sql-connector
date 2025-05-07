const config = require('../../config')
const axios = require('axios');
const jwt = require('jsonwebtoken');
const authConfig = config.authConfig
const keycloakServerURL = authConfig.idmHost;
const realm = authConfig.authRealm;
const clientID = authConfig.clientId;
const clientSecret = authConfig.secret;
const {parseJwt} = require("../../utils/common")
const logger = require('percocologger')

module.exports = {
    auth: async (req, res, next) => {

        if (req.body.file)
            req.body = JSON.parse(req.body.file)

        if (!req.headers.visibility)
            req.headers.visibility = "private"

        if (req.headers.visibility == "public" && req?.body?.query && !req?.body?.query?.toLowerCase().includes("public-data"))
            return res.status(400).send("If you are requesting for public files you must search in public-data bucket");

        if (req?.body?.query?.toLowerCase().includes("select * from public-data"))
            req.body.query = req.body.query.replace("SELECT * FROM public-data", "SELECT * FROM publicdata")

        if (authConfig.disableAuth)
            next()

        else {
            let authHeader = req.headers.authorization;

            if (authHeader) {
                if (authHeader && !authHeader.startsWith("Bearer"))
                    authHeader = "Bearer " + authHeader

                const jwtToken = authHeader.split(' ')[1];

                let verifiedToken
                try {
                    verifiedToken = jwt.verify(jwtToken, //Buffer.from(
                        authConfig.publicKey
                        //, 'base64').toString()
                        //-------//
                        , { algorithms: ['RS256'] })
                }
                catch (error) {

                    logger.error(error)
                    if (error.message == "invalid token" || error.message == "jwt expired" || error.message == "jwt malformed")
                        return res.sendStatus(403);
                    else
                        return res.sendStatus(500);
                }


                if (authConfig.introspect) {
                    const introspectionEndpoint = `${keycloakServerURL}/realms/${realm}/protocol/openid-connect/token/introspect`;

                    const data = new URLSearchParams();
                    data.append('token', jwtToken);
                    data.append('client_id', clientID);
                    data.append('client_secret', clientSecret);

                    axios.post(introspectionEndpoint, data)
                        .then(response => {
                            if (response.data.active) {
                                logger.info('Token valid:', response.data);
                                next();
                            } else {
                                logger.error('Token not valid.');
                                return res.sendStatus(403);
                            }
                        })
                        .catch(error => {
                            logger.error(error.response.data)
                            logger.error('Errore during token verify:', error.message);
                            return res.sendStatus(500);
                        });
                }
                else {

                    const decodedToken = verifiedToken || parseJwt(jwtToken)

                    if ((decodedToken.azp == authConfig.clientId) && ((decodedToken.exp * 1000) > Date.now())) {

                        try {
                            let data = (await axios.get(config.authConfig.userInfoEndpoint, { headers: { "Authorization": authHeader } })).data
                            let { pilot, username, email } = data
                            req.body.bucketName = pilot.toLowerCase() //+ "/" + email + "/" + config.minioConfig.defaultInputFolderName//{pilot, email}
                            req.body.prefix = (email || username) + "/" + config.minioConfig.defaultInputFolderName
                            config.group = email || username
                            logger.debug(req.body.prefix)
                        }
                        catch (error) {
                            logger.error(error?.toString())
                            logger.error(error?.response?.data || error?.response)
                            req.body.prefix = decodedToken.email
                            config.group = decodedToken.email
                        }

                        //if (req.params.bucketName && req.params.objectName)
                        //    logger.debug(req.body.bucketName , req.params.bucketName , req.body.prefix , req.params.objectName.split("/")[0] + "/" + req.params.objectName.split("/")[1])
                        let deniedQuery, query
                        try {
                            if (req.body.query) {
                                query = req.body.query.split("FROM ")
                                query = query[1].split(" ")
                                if (query[0] != req.body.bucketName)
                                    deniedQuery = true
                                query = req.body.query.split("name = '")[1]
                                if (!query.startsWith(req.body.prefix))
                                    deniedQuery = true
                            }
                            //if (!deniedQuery)
                            next()
                            //else 
                            //    res.status(403).send("Available bucketname is " + req.body.bucketName + " and you tried to access " + req.body.query.split("FROM ")[1].split(" ") + ".\nAvailable prefix is " + req.body.prefix + " and you tried to access this object " + req.body.query.split("name = '")[1].split("'"));
                        }
                        catch (error) {
                            logger.error(error)
                            next()
                        }
                    }
                    else
                        return res.sendStatus(403);
                }
            }
            else
                return res.sendStatus(401);
        }
    }
};