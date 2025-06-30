# -------- Build Stage -------------------------------------------
FROM node:18 AS build

WORKDIR /usr/src/app
COPY Homework2/tech_prototype/frontend/package*.json ./
RUN npm install
COPY Homework2/tech_prototype/frontend/ ./
RUN npm run build            # /usr/src/app/build

# -------- Production Stage --------------------------------------
FROM nginx:alpine
COPY --from=build /usr/src/app/build /usr/share/nginx/html
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
