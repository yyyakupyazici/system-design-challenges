# syntax=docker/dockerfile:1
# Lab ajanı (mini-Patroni). Gerçek kurulumda her PostgreSQL pod'unun yanında sidecar olarak çalışırdı (Patroni gibi).
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 mvn -q -B dependency:go-offline
COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -q -B -DskipTests package \
 && java -Djarmode=tools -jar target/raft-etcd-lab-*.jar extract --layers --launcher --destination /extracted

FROM eclipse-temurin:21-jre
RUN useradd --system --uid 10001 app
WORKDIR /app
COPY --from=build /extracted/dependencies/ ./
COPY --from=build /extracted/spring-boot-loader/ ./
COPY --from=build /extracted/snapshot-dependencies/ ./
COPY --from=build /extracted/application/ ./
USER 10001
ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
