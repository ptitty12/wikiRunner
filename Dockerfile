FROM nginx:alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY wikirunner.html /usr/share/nginx/html/index.html