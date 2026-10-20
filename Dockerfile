# syntax=docker/dockerfile:1
FROM eclipse-temurin:25-jdk-jammy@sha256:07a4ee373cd86f337e62cb8149f1e0a9f08aaf3a18c27509a493dda119fb480c AS build
WORKDIR /workspace
COPY .mvn .mvn
COPY mvnw pom.xml ./
RUN chmod +x mvnw
COPY src src
# validate incluye Spotless. Las pruebas con Docker se ejecutan mediante verify en el host.
RUN --mount=type=cache,target=/root/.m2 ./mvnw -B -ntp -DskipTests package

FROM eclipse-temurin:25-jre-jammy@sha256:e0d8c47eccd7ec82d635542dfcd1f2b1293add6bdd7ffbeb650f8ec1b1335425 AS application
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /workspace/target/max-capital-challenge-0.1.0-SNAPSHOT.jar app.jar
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

FROM docker:29.0.0-cli@sha256:87858a6dd14149240f4a2c33e0ec0f32a5ac1bb2548c04f5a20694ea25cb6157 AS docker-cli
FROM application AS evaluation-console
USER 0:0
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
ENV SPRING_PROFILES_ACTIVE=evaluation-console

# El target por defecto conserva la aplicación sin CLI ni acceso al motor.
FROM application AS default
