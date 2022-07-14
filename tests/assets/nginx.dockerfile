FROM nginx:1.23.0-alpine
COPY static.conf /etc/nginx/conf.d/default.conf
