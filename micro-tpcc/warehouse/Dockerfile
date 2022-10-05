#
# Build stage
# https://hub.docker.com/_/maven
#
FROM maven:3.8-openjdk-18-slim AS build
COPY src /home/app/src
COPY pom.xml /home/app
RUN mvn -f /home/app/pom.xml clean package


#
# Package stage
# https://hub.docker.com/_/openjdk/tags
#
#FROM openjdk:18
FROM openjdk:18-slim-bullseye
COPY --from=build /home/app/target/warehouse-1.0-SNAPSHOT.jar /usr/local/lib/warehouse.jar
EXPOSE 8081
ENTRYPOINT ["java","-jar","/usr/local/lib/warehouse.jar"]