FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
RUN mkdir -p /app/results
COPY experiments/target/experiments-*.jar app.jar
EXPOSE 8090
ENTRYPOINT ["java", "--enable-preview", "-XX:MaxRAMPercentage=75.0", \
            "-Dspring.profiles.active=docker", "-jar", "app.jar"]
