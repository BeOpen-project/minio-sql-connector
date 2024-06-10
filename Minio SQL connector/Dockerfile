FROM node:16-alpine
WORKDIR /app
COPY . .
RUN ls
RUN cd "./Minio SQL Connector"
RUN npm install
CMD ["node", "index"]
