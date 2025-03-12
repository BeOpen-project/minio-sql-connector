const common = require("./utils/common")
const config = common.checkConfig(require('./config'))
logger.log(config.queryAllowedExtensions)
const express = require('express');
const bodyParser = require('body-parser');
const app = express();
const port = config.port || 3000;
const mongoose = require("mongoose");
const cors = require('cors');
const routes = require ("./api/routes/router")
const Log = require('./utils/logger')//.app(module);
const { Logger } = Log
const logger = new Logger("service")

app.use(cors());
app.use(express.urlencoded({ extended: false }));
app.use(bodyParser.json());
app.use(config.basePath || "/api", routes);
app.listen(port, () => {logger.log(`Server listens on http://localhost:${port}`);});
mongoose.connect(config.mongo, { useNewUrlParser: true }).then(() => {logger.log("Connected to mongo")})
logger.log(`Node.js version: ${process.version}`);