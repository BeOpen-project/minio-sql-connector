FROM node:16-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY ./Minio-SQL-Connector/* ./* 
CMD ["node", "index"]
