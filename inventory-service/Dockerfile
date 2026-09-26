FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY . .
RUN mvn -B -q -pl inventory-service -am package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/inventory-service/target/inventory-service-*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
