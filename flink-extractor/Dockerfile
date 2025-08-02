FROM maven:3.8.5-openjdk-17-slim AS builder

WORKDIR /build
COPY flink-extractor/pom.xml .
RUN mvn dependency:go-offline -B

COPY flink-extractor/src src
RUN mvn clean package -DskipTests

# Verify the JAR was created and show its contents
RUN ls -la /build/target/ && \
    if [ -f /build/target/flink-extractor-1.0-SNAPSHOT.jar ]; then \
        echo "JAR created successfully" && \
        jar tf /build/target/flink-extractor-1.0-SNAPSHOT.jar | head -20; \
    else \
        echo "JAR not found!" && exit 1; \
    fi

FROM flink:2.0.0-scala_2.12-java21

# Copy the JAR file
COPY --from=builder /build/target/flink-extractor-1.0-SNAPSHOT.jar /opt/flink/usrlib/

ENV ENVIRONMENT="dev11"
ENV DW_HOST="postgres-dw"
ENV DW_PORT="5433"
ENV DW_DB="dw_db"
ENV DW_USERNAME="postgres"
ENV DW_PASSWORD="postgres"
ENV GROUP_ID="test2223"

# Verify the JAR was copied
RUN ls -la /opt/flink/usrlib/

WORKDIR /