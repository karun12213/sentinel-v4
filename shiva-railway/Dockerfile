FROM node:20-slim

WORKDIR /app

COPY package.json ./
RUN npm install --production

COPY . .

CMD ["node", "shiva_live_hyperscale.js"]
