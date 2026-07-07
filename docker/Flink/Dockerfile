FROM maven:3.9.16-eclipse-temurin-17-alpine AS builder

WORKDIR /app
COPY src/flink_job/pom.xml .
RUN mvn dependency:go-offline
COPY src/flink_job/src ./src
RUN mvn package -DskipTests


FROM flink:1.20.4-java17

RUN mkdir -p /opt/flink/plugins/influxdb && \
    wget -P /opt/flink/plugins/influxdb/ https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-influxdb/1.20.4/flink-metrics-influxdb-1.20.4.jar

COPY --from=builder /app/target/flink_job-1.jar /opt/flink/usrlib/
