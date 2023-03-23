logserver-spark
===============

Сервер расчёт ионосферных параметров на основе Apache Spark.

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
