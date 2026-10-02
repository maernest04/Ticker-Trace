FROM node:20-alpine AS dependencies

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

FROM node:20-alpine AS build

WORKDIR /app

ARG API_URL=http://api:8000
ENV API_URL=$API_URL
ARG NEXT_PUBLIC_API_ORIGIN=http://localhost:8000
ENV NEXT_PUBLIC_API_ORIGIN=$NEXT_PUBLIC_API_ORIGIN

COPY --from=dependencies /app/node_modules ./node_modules
COPY . ./
RUN npm run build

FROM node:20-alpine

WORKDIR /app

ENV NODE_ENV=production

COPY --from=build /app ./

EXPOSE 3000

CMD ["npm", "run", "start"]
