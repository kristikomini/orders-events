FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY . .
RUN mvn -B -q -pl shipment-service -am package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/shipment-service/target/shipment-service-*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
