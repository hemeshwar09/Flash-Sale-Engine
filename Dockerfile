FROM node:22-alpine
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY src ./src
COPY public ./public
COPY loadtest ./loadtest
USER node
EXPOSE 3000
CMD ["node", "src/server.js"]
