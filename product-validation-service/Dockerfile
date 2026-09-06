FROM eclipse-temurin:21-jre
WORKDIR /app
COPY target/product-validation-service-1.0.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java","-jar","/app/app.jar"]
