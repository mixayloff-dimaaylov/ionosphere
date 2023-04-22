logserver-spark
===============

Сервер расчётов ионосферных параметров на основе Apache Spark.

## Docker-образ spark-TecCalculationV2

spark-TecCalculationV2 -- основной Docker-образ для вычисления характеристик
ПЭС. Исполняемый файл принимает аргументы коммадной строки. Docker-образ
принимает параметры в виде переменных окружения и запускает сессию Spark с
нужными аргументами. Для запуска необходимо определить 5 переменных:

- `REC_LAT`, `REC_LON`, `REC_ALT` -- широта, долгота и высота GPS-приёмника над
  уровнем моря соответственно. Используются для преобразования координат и
  расчета подспутниковых и подионосферных точек

- `KAFKA_HOST` -- IPv4-адрес шины Kafka для приёма сообщений NovAtelLogReader

- `CH_HOST` -- IPv4-адрес базы данных ClickHouse для сохранения результатов
  рассчетов

Необходимо предварительно собрать образ:

```sh
docker build -t logserver-spark:latest .
```

Запуск производиться с передачей переменных окружения:

```sh
docker run -d -e REC_LAT=<...> -e REC_LON=<...> -e REC_ALT=<...> \
              -e KAFKA_HOST=<...> -e CH_HOST=<...> \
              logserver-spark:latest
```

## Профилирование памяти при помощи VisualVM

Для профилирования необходимо:

1. Запустить агент профилирования JMX на стороне Spark, передав драйверу
   соответвующие аргументы коммандной строки:

   ```sh
   spark-submit \
       --class com.infocom.examples.spark.TecCalculationV2 \
       <прочие аргументы> ... \
       --conf "spark.driver.extraJavaOptions=-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=8090 -Dcom.sun.management.jmxremote.rmi.port=8091 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=<внешний_адрес>" \
       <исполняемый_файл.jar> \
       <прочие_аргументы_исполняемого_файла> ...
   ```

   Для этого необходимо модифицировать аргументы внутри Docker-файла.

   Таким же образом можно профилировать узлы-исполнители.

   Назначение аргументов:

   - `-Dcom.sun.management.jmxremote` -- активирует JMX
   - `-Dcom.sun.management.jmxremote.port=8090` -- порт сервера
   - `-Dcom.sun.management.jmxremote.rmi.port=8091` -- RMI-порт сервера
   - `-Dcom.sun.management.jmxremote.authenticate=false` -- аутентификация
     (по-умолчанию не требуется)
   - `-Dcom.sun.management.jmxremote.ssl=false` -- шифрование с использованием
     SSL-сертификатов (по-умолчанию не требуется)
   - `-Djava.rmi.server.hostname=<внешний_адрес>` -- внешний адрес сервера
