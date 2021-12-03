FROM mozilla/sbt:8u171_0.13.13 AS build
WORKDIR '/usr/local/src/spark'
COPY build.sbt .
COPY project project/
RUN sbt update

COPY . .
RUN sbt projectAssembly/assembly

FROM bde2020/spark-base:2.2.1-hadoop2.7 AS install
COPY --from=build /usr/local/src/spark/assembly/target/scala-2.11/novatel-streaming-assembly-*.jar /spark/jars/

FROM install AS spark-streamer-1
# Kafka client ports
# ClickHouse client ports
CMD /spark/bin/spark-submit \
	--deploy-mode client \
        --master local[*] \
	--class com.infocom.examples.spark.StreamReceiver \
	--driver-memory 512m \
	--num-executors 1 \
	--executor-cores 2 \
	--executor-memory 1500m \
	--conf spark.locality.wait=10 \
	--conf spark.task.maxFailures=8 \
	--conf spark.yarn.maxAppAttempts=4 \
	--conf spark.yarn.am.attemptFailuresValidityInterval=1h \
	--conf spark.yarn.max.executor.failures=8 \
	--conf spark.yarn.executor.failuresValidityInterval=1h \
	/spark/jars/novatel-streaming-assembly-1.0.jar \
	$KAFKA_HOST:9092 $CH_HOST:8123

FROM install AS spark-streamer-2
# Kafka client ports
# ClickHouse client ports
CMD /spark/bin/spark-submit \
	--deploy-mode client \
        --master local[*] \
	--class com.infocom.examples.spark.TecCalculation \
	--driver-memory 512m \
	--num-executors 1 \
	--executor-cores 2 \
	--executor-memory 1500m \
	--conf spark.locality.wait=10 \
	--conf spark.task.maxFailures=8 \
	--conf spark.yarn.maxAppAttempts=4 \
	--conf spark.yarn.am.attemptFailuresValidityInterval=1h \
	--conf spark.yarn.max.executor.failures=8 \
	--conf spark.yarn.executor.failuresValidityInterval=1h \
	/spark/jars/novatel-streaming-assembly-1.0.jar \
	$CH_HOST:8123 120000
        # ClickHouse # delay
