# Copyright 2023 mixayloff-dimaaylov at github dot com
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM mozilla/sbt:8u292_1.5.7 AS build
WORKDIR '/usr/local/src/spark'
COPY build.sbt .
COPY project project/
RUN sbt update

COPY . .
RUN sbt projectAssembly/assembly

FROM bde2020/spark-base:3.3.0-hadoop3.3 AS install
COPY --from=build /usr/local/src/spark/assembly/target/scala-2.12/novatel-streaming-assembly-*.jar /spark/jars/
COPY ./bin/avro/ /spark/avro-schemas/

FROM install AS spark-TecCalculationV2
# KAFKA_HOST: Kafka host:port
# CH_HOST: ClickHouse host:port
CMD /spark/bin/spark-submit \
    --deploy-mode client \
    --master local[*] \
    --class com.infocom.examples.spark.TecCalculationV2 \
    --driver-memory 2g \
    --num-executors 1 \
    --executor-cores 2 \
    --executor-memory 1500m \
    --conf spark.locality.wait=10 \
    --conf spark.task.maxFailures=8 \
    --conf spark.yarn.maxAppAttempts=4 \
    --conf spark.yarn.am.attemptFailuresValidityInterval=1h \
    --conf spark.yarn.max.executor.failures=8 \
    --conf spark.yarn.executor.failuresValidityInterval=1h \
    --conf spark.sql.shuffle.partitions=1 \
    /spark/jars/novatel-streaming-assembly-1.0.jar \
    $REC_LAT $REC_LON $REC_ALT \
    $KAFKA_HOST:9092 $CH_HOST:8123
