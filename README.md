# BigDataFlink
Анализ больших данных - лабораторная работа №3 - Streaming processing с помощью Flink

Одним из самых популярных фреймворков для работы со streaming processing является Apache Flink. Apache Flink - мощный фреймворк, который предлагает широкий набор функциональности для простого написания streaming processing.

Что необходимо сделать? 

Необходимо реализовать потоковую обработку данных с помощью Flink, который читает топик Kafka, трансформирует данные в режиме streaming в модель звезда и пишет результат в PostgreSQL. Данные в Kafka-топиках хранятся в формате json. Данные в топик kafka нужно отправлять самостоятельно, эмулируя источник данных.

Какие данные отправляются в Kafka?
 - Каждое сообщение в Kafka-топике - это строчка из csv файлов, преобразованная в формат json.

Какие данные отправляются в PostgreSQL?
 - Трансформированные данные в модель данных звезда.

![Лабораторная работа №3](https://github.com/user-attachments/assets/d3c1544d-3fe6-4c15-b673-9aa5d27dbd76)


Алгоритм:

1. Клонируете к себе этот репозиторий.
2. Устанавливаете инструмент для работы с запросами SQL (рекомендую DBeaver).
3. Устанавливаете базу данных PostgreSQL (рекомендую установку через docker).
4. Устанавливаете Apache Flink (рекомендую установку через Docker).
5. Устанавливаете Apache Kafka (рекомендую установку через Docker).
6. Скачиваете файлы с исходными данными mock_data( * ).csv, где ( * ) номера файлов. Всего 10 файлов, каждый по 1000 строк.
7. Реализуете приложение, которое каждую строчку из исходных csv-файлов преобразует в json и отправляет в виде сообщения в Kafka-топик.
8. Реализуете приложение на Flink, которое читает Kafka-топик, преобразует данные в модель звезда и сохраняет в PostgreSQL в режиме streaming.
9. Проверяете конечные данные в PostgreSQL.
10. Отправляете работу на проверку лаборантам.

Что должно быть результатом работы?

1. Репозиторий, в котором есть исходные данные mock_data().csv, где () номера файлов. Всего 10 файлов, каждый по 1000 строк.
2. Файл docker-compose.yml с установкой PostgreSQL, Flink, Kafka и запуском приложения, которое из файлов mock_data(*).csv создает сообщения json в Kafka.
3. Инструкция, как запускать Flink-джобу и приложение для отправки данных в Kafka для проверки лабораторной работы.
4. Код Apache Flink для трансформации данных в режиме streaming.

---

## Как запустить

### 1. Поднять всю инфраструктуру

```bash
docker compose up -d
```

Поднимаются: Kafka в KRaft-режиме без Zookeeper (на `localhost:9094` снаружи),
PostgreSQL (`localhost:5432`, db/user/pass = `lab`),
Flink JobManager (UI на http://localhost:8081), TaskManager.
PostgreSQL автоматически создаст таблицы из `schema.sql` (звезда: `dim_customer`,
`dim_seller`, `dim_product`, `dim_store`, `dim_supplier`, `fact_sales`).

Сервис `producer` запустит `extract.py`, который читает все csv из `исходные данные/`
и отправляет каждую строку как json в kafka-топик `sales`.

### 2. Скачать jar-коннекторы для Flink

Положить в папку `./jars/` рядом с `docker-compose.yml`:

- `flink-sql-connector-kafka-3.1.0-1.18.jar`
- `flink-connector-jdbc-3.1.2-1.18.jar`
- `postgresql-42.7.3.jar`

```
mkdir -p jars && cd jars

# Kafka SQL connector (Flink 1.18)
curl -LO https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

# JDBC connector
curl -LO https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar

# PostgreSQL JDBC driver
curl -LO https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar
```

### 3. Запустить Flink-джобу

```bash
docker compose exec jobmanager ./bin/sql-client.sh -f /opt/flink/flink_job.sql
```

### 4. Проверить результат

```bash
docker compose exec postgres psql -U lab -d lab -c "SELECT count(*) FROM fact_sales;"
```

### Локальный запуск продюсера (без docker)

```bash
pip install -r requirements.txt
KAFKA_BOOTSTRAP=localhost:9094 python extract.py
```
