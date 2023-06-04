# Change Log

## _unreleased_

### Changed

- Добавлены колонки `geopointStr`, `ionpointStr` со строковым GeoHash в таблице
  `rawdata.satxyz2` для работы плагина Grafana GeoMap
  ([#23](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/23))

- Теперь колонки `avgNT`, `delNT` в таблице `computed.NTDerivatives` содержат
  вертикальный ПЭС $N_T$
  ([#22](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/22))

### Added

- Добавлен расчет вертикального ПЭС $N_T$
  ([#22](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/22))

### Fixed

### Removed

## [0.3.0] - 2023-04-29

### Changed

- **breaking(V2):** Теперь для десериализации сообщений NovAtelLogReader из
  Apache Kafka используется Avro-схемы (`*.avsc`-файлы), из-за разрыва обратной
  совместимости со стороны Apache Avro
  ([#15](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/15))

- **breaking(V1, V2):** Адрес ClickHouse и координаты GPS-приёмника теперь
  являются **обязательными** аргументами коммадной строки (CLI)
  ([`8e059c0`](https://github.com/mixayloff-dimaaylov/logserver-spark/commit/8e059c0),
  [#19](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/19))

- **dependecies:** `sbt` обновлён с `0.13.13` до `1.5.7`

- **dependecies:** Scala обновлена с `2.11.8` до `2.12.17`

- **dependecies:** Apache Spark обновлен с `2.2.1-hadoop2.7` до
  `3.3.0-hadoop3.3`

- **breaking(API):** Функции `f1()` и `f2()` объединены в общую функцию `f()`
  ([#4](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/4),
  [#17](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/17))

- **breaking(API):** Версия таблиц ClickHouse обновлена с 6 до 16.1

- Теперь проект лицензируется под лицензией Apache 2.0
  ([#16](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/16))

### Added

- **V2:** Алгоритм расчёта ПЭС и характеристик ионосферы портирован на
  _Structured Streaming_
  ([#15](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/15),
  [#18](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/18))

- Добавлена поддержка сборки Docker-контейнера
  ([`2cc0e52`](https://github.com/mixayloff-dimaaylov/logserver-spark/commit/2cc0e52),
  [#1](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/1))

- Возвращены старые логи `ISMRAWTECB`, `ISMDETOBSB`, `ISMREDOBSB` для целей
  отладки ([#2](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/2))

- Добавлен расчет $DNT$ с учётом $RDBC$
  ([#5](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/5),
  [#7](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/7))

- Добавлен рассчёт $N_{T adr}$ в целях отладки
  ([#14](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/14))

- Добавлен рассчёт $S_{4 C/No}$ в целях отладки
  ([#13](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/13))

- Добавлена поддержка Ammonite, Bloop, LSP Metals
  ([#6](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/6))

- **docs:** Добавлена инструкция по профилированию приложения при помощи
  VisualVM
  ([#15](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/15))

- **docs:** Добавлен CHANGELOG
  ([#20](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/20))

### Fixed

- **V1, V2:** Адрес ClickHouse и координаты GPS-приёмника теперь можно указывать
  в виде аргументов коммадной строки
  ([`8e059c0`](https://github.com/mixayloff-dimaaylov/logserver-spark/commit/8e059c0),
  [#19](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/19))

- Исправлено преобразование частот
  ([`c347760`](https://github.com/mixayloff-dimaaylov/logserver-spark/commit/c347760),
  [#4](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/4),
  [#17](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/17))

- Исправлена работа Kafka при подключении нескольких кластеров к общей шине
  ([#3](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/3))

- Исправлена проблема всплесков фильтров, мешающих обнаружению МИО, путем
  отсечки начальных значений фильтров
  ([#8](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/8))

- Исправлены срывы ПЭС из-за неправильной синхронизации вычислителя и ClickHouse
  ([#9](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/9),
  [#10](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/10))

- Исправлено неверное выражение $S_4$
  ([#11](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/11),
  [#12](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/12))

- Исправлена степень в выражении $\Sigma_{\varphi}$
  ([#12](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/12))

- Исправлены конфликты завимостей при сборке FatJar
  ([#15](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/15))

### Deprecated

- **V1:** Предыдущая версия вычислителя теперь в состоянии поддержки и может
  быть удалена в дальнейшем
  ([#15](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/15))

- **V1:** `ClickHouseStreamReceiver` находится в нерабочем состоянии из-за
  разрыва совместимости в схемах Avro и будет удалён в дальнейшем как замещенный
  TecCalculationV2
  ([#15](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/15))

- **API:** Функции `f1()` и `f2()` считаются устаревшими в пользу `f()`
  ([#4](https://github.com/mixayloff-dimaaylov/logserver-spark/pull/4))

### Removed

- **docs:** Файл `db_layout.md` перенесён в основной проект

## [0.2.0] - 2020-08-24

:seedling: Initial release.

[0.3.0]: https://github.com/mixayloff-dimaaylov/logserver-spark/releases/tag/0.3.0
[0.2.0]: https://github.com/mixayloff-dimaaylov/logserver-spark/releases/tag/0.2.0
