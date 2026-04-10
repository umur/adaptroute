FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY order-service/target/order-service-*.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "--enable-preview", "-XX:+UseZGC", "-XX:MaxRAMPercentage=75.0", \
            "-Dspring.profiles.active=docker", "-jar", "app.jar"]
