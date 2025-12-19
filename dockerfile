FROM node:24-alpine

WORKDIR /usr/src/app

COPY source .

COPY package*.json .

RUN npm install

CMD ["npm", "start"]
