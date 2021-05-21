FROM mozilla/sbt:8u171_0.13.13 AS build
WORKDIR '/usr/local/src/spark'
COPY . .
RUN sbt projectAssembly/assembly

FROM bde2020/spark-base:2.2.1-hadoop2.7
COPY --from=build /usr/local/src/spark/assembly/target/scala-2.11/novatel-streaming-assembly-*.jar /spark/jars/
CMD /bin/sh
