FROM eclipse-temurin:21-jre-alpine
LABEL authors="boglioli, colombi, galimberti"

# Install the traffic control tool for network emulation
RUN apk add --no-cache iproute2

WORKDIR /app

# Copy the compiled fat-jar into the container
COPY target/fault-tolerant-pubsub-1.0-SNAPSHOT.jar /app/app.jar

RUN mkdir -p /app/data