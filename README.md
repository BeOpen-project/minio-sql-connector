# Minio SQL connector

Rename docker.compose.prod.yml in docker.compose.yml and config.template.js in config.js, so you can set minio and postgre credentials without pushing it because they are in gitignore.

Send a POST request to http://localhost:3000/api/query with the beopen bearer token in header and the postgre query in payload:
{
    "query" : "the query..."
} 

# Trino connector

This is a tool using trino to do Minio queries. It is integrated with Keycloak