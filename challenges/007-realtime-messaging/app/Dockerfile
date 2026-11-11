# syntax=docker/dockerfile:1
# Tek imaj, iki rol: gateway (WebSocket) ve worker (relay + fan-out + kart projeksiyonu). Rol ROLE_* env ile seçilir.
# Build context = 007 klasörü (üst pom iki modülü birlikte bilir):  docker build -f app/Dockerfile .
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
COPY payment-request-service/pom.xml payment-request-service/
COPY payment-request-service/src payment-request-service/src
COPY app/pom.xml app/
COPY app/src app/src
RUN --mount=type=cache,target=/root/.m2 mvn -q -B -DskipTests -pl app -am package \
 && java -Djarmode=tools -jar app/target/realtime-messaging-*.jar extract --layers --launcher --destination /extracted

FROM eclipse-temurin:21-jre
RUN useradd --system --uid 10001 app
WORKDIR /app
COPY --from=build /extracted/dependencies/ ./
COPY --from=build /extracted/spring-boot-loader/ ./
COPY --from=build /extracted/snapshot-dependencies/ ./
COPY --from=build /extracted/application/ ./
USER 10001
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
