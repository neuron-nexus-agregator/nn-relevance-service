FROM debian:latest
LABEL maintainer="gafarov@realnoevremya.ru"
RUN apt-get update -y && apt-get upgrade -y
RUN apt-get install -y ca-certificates
COPY . .
WORKDIR /build/linux
CMD [ "./relevance-service" ]