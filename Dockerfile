FROM maven:3.9.12-eclipse-temurin-17 AS build
WORKDIR /workspace

COPY pom.xml ./
RUN mvn -q -DskipTests dependency:go-offline

COPY src ./src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /workspace/target/cluster-sync.jar /app/cluster-sync.jar

ENTRYPOINT ["java", "-cp", "/app/cluster-sync.jar"]

