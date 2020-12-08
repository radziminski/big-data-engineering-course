#############################################################
# build image
#############################################################
# use
#   docker-compose build stream-data-preprocessor
# to build the image
#############################################################
FROM maven:3.6.3-openjdk-8 as mavenBuild

SHELL ["/bin/bash", "-c"]

# pre-fetch dependencies - disabled
# RUN if [[ -n ${HTTPS_PROXY} ]]; then \
#     proxy=($(sed 'y/:/ /' <<<"${HTTPS_PROXY/http\:\/\/}")) && \
#     mvn dependency:resolve -B -f /tmp/pom.xml -s /usr/share/maven/ref/settings-docker.xml -Dhttps.proxyHost=${proxy[0]} -Dhttps.proxyPort=${proxy[1]}; \
#   else \
#     #mvn dependency:resolve -B -f /tmp/pom.xml -s /usr/share/maven/ref/settings-docker.xml; \
#     mvn clean package -f /tmp/pom.xml -s /usr/share/maven/ref/settings-docker.xml -Dmaven.main.skip -Dmaven.test.skip; \
#   fi

#RUN mvn clean package -Dmaven.main.skip -Dmaven.test.skip -f /tmp/pom.xml -s /usr/share/maven/ref/settings-docker.xml && rm -r target
#FROM mavenDep as mavenBuild

# copy pom file
COPY ./pom.xml ./pom.xml

# copy your other files
COPY ./src ./src

RUN ls /usr/share/maven/ref/

# build for release
# build all dependencies
RUN if [[ -n ${HTTPS_PROXY} ]]; then \
    proxy=($(sed 'y/:/ /' <<<"${HTTPS_PROXY/http\:\/\/}")) && \
    mvn package -Dhttps.proxyHost=${proxy[0]} -Dhttps.proxyPort=${proxy[1]}; \
  else \
    mvn package; \
  fi


#############################################################
# run image
#############################################################

FROM openjdk:8u212-jre-alpine

RUN apk update && apk add bash
ADD https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh /usr/local/bin/ 
RUN chmod a+x /usr/local/bin/wait-for-it.sh

RUN mkdir -p /var/iot-producer
WORKDIR /var/iot-producer

# copy over the built artifact from the maven image
# TODO: change jar name accoring to current version
COPY --from=mavenBuild target/kafkaproducerconsumer-0.0.1-SNAPSHOT.jar /var/iot-producer/app.jar

# commented started from docker-compose file
#ENTRYPOINT [ "java", "-jar", "app.jar" ]
