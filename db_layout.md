clickhouse database layout
=============================

*Примечание:* для поддержки TTL необходима версия clickhouse>=19.6(1.1.54370)

### Таблицы для входных данных

Таблицы, формируемые `logreader`.

#### rawdata.range

Источник: `logreader`  

```sql
CREATE TABLE rawdata.range (
  time UInt64,
  adr Float64,
  psr Float64,
  cno Float64,
  locktime Float64,
  sat String,
  system String,
  freq String,
  glofreq Int32,
  prn Int32,
  d Date MATERIALIZED toDate(round(time / 1000))
) ENGINE = MergeTree(d, (time, sat, freq), 8192)
TTL d + INVERVAL 2 WEEK DELETE
```

#### rawdata.satxyz2

Источник: `logreader`  

```sql
CREATE TABLE rawdata.satxyz2 (
  time UInt64,
  geopoint UInt64,
  ionpoint UInt64,
  elevation Float64,
  sat String,
  system String,
  prn Int32,
  d Date MATERIALIZED toDate(round(time / 1000))
) ENGINE = MergeTree(d, (time, sat), 8192)
TTL d + INVERVAL 2 WEEK DELETE
```

### Таблицы для расчетных данных

#### Обычные таблицы

Источник: *rawdata.range*

```sql
CREATE TABLE computed.NT (
    time UInt64, --метка времени (timestamp в ms)
    sat String, --спутник
    sigcomb String, --комбинация сигналов
    f1 Float64, --частота 1
    f2 Float64, --частота 2
    nt Float64, --ПЭС
    d Date MATERIALIZED toDate(round(time / 1000))
) ENGINE = ReplacingMergeTree(d, (time, sat, sigcomb), 8192)
TTL d + INTERVAL 2 Week DELETE;
```

Источник: *computed.NT*

```sql
CREATE TABLE computed.NTDerivatives (
    time UInt64, --метка времени (timestamp в ms)
    sat String, --спутник
    sigcomb String, --комбинация сигналов
    f1 Float64, --частота 1
    f2 Float64, --частота 2
    avgNT Float64, --среднее значение ПЭС
    delNT Float64, --значение флуктуаций ПЭС
    d Date MATERIALIZED toDate(round(time / 1000))
) ENGINE = ReplacingMergeTree(d, (time, sat, sigcomb), 8192) 
TTL d + INTERVAL 2 Week DELETE;
```

#### Односекундные таблицы

Источник: *computed.NTDerivatives*

```sql
CREATE TABLE computed.xz1 (
    time UInt64, --метка времени (timestamp в ms)
    sat String, --спутник
    sigcomb String, --комбинация сигналов
    f1 Float64, --частота 1
    f2 Float64, --частота 2
    sigNT Float64, --значение СКО флуктуаций ПЭС
    sigPhi Float64, --значение СКО флуктуаций фазы на фазовом экране
    gamma Float64, --значение параметра Райса (глубины общих замираний)
    Fc Float64, --значение интервала частотной корреляции
    Pc Float64, --значение интервала пространственной корреляции
    d Date MATERIALIZED toDate(round(time / 1000))
) ENGINE = ReplacingMergeTree(d, (time, sat, sigcomb), 8192) 
TTL d + INTERVAL 2 Week DELETE;
```

Источник: *rawdata.range*

```sql
CREATE TABLE computed.s4 (
    time UInt64, --метка времени (timestamp в ms)
    sat String, --спутник
    freq String, --частота, для которой рассчитано значение
    s4 Float64, --S4
    d Date MATERIALIZED toDate(round(time / 1000))
) ENGINE = ReplacingMergeTree(d, (time, sat, freq), 8192)
TTL d + INTERVAL 2 Week DELETE
```

#### N - секундные таблицы

Источник: *computed.NTDerivatives*

```sql
CREATE TABLE computed.Tc (
    time UInt64, --метка времени (timestamp в ms)
    sat String, --спутник
    sigcomb String, --комбинация сигналов
    Tc Float64, --значение интервала временной корреляции
    d Date MATERIALIZED toDate(round(time / 1000))
) ENGINE = ReplacingMergeTree(d, (time, sat, sigcomb), 8192)
TTL d + INTERVAL 2 Week DELETE
```