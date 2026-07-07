FROM maven:3.9.16-eclipse-temurin-17-alpine AS builder

WORKDIR /app
COPY src/flink_job/pom.xml .
RUN mvn dependency:go-offline
COPY src/flink_job/src ./src
RUN mvn package -DskipTests


FROM flink:1.20.4-java17

RUN mkdir -p /opt/flink/plugins/influxdb && \
    cp /opt/flink/opt/flink-metrics-influxdb-*.jar /opt/flink/plugins/influxdb/

COPY --from=builder /app/target/flink_job-1.jar /opt/flink/usrlib/
