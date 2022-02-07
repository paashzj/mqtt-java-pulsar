FROM ttbb/compile:jdk11-git-mvn AS build
COPY . /opt/sh/compile
WORKDIR /opt/sh/compile
RUN mvn -B clean package


FROM ttbb/base:jdk11

LABEL maintainer="shoothzj@gmail.com"

COPY --from=build /opt/sh/compile/target/mqtt-java-pulsar-0.0.1-SNAPSHOT-jar-with-dependencies.jar /opt/sh/mqtt-pulsar.jar

COPY docker-build /opt/sh

CMD ["/usr/bin/dumb-init", "bash", "-vx","/opt/sh/scripts/start.sh"]
