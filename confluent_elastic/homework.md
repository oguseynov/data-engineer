# Домашняя работа по Confluent и Elastic

## 2
#### Посмотрите какие топики есть сейчас в системе, и на основе того, в котором вы видите максимальный объем данных создайте stream по имени WIKILANG который фильтрует правки только в разделах национальных языков, кроме английского (поле channel вида #ru.wikipedia), который сделали не боты. Stream должен содержать следующие поля: createdat, channel, username, wikipage, diffurl

```bash
CREATE STREAM wikilang WITH (PARTITIONS=2,REPLICAS=2) AS SELECT CREATEDAT, CHANNEL, USERNAME, WIKIPAGE, DIFFURL FROM wikipedia WHERE isbot <> true AND channel <> '#en.wikipedia';
```
 Скриншот консоли будет ниже.

## 3
#### После 1-2 минут работы откройте Confluent Control Center и сравните пропускную способность топиков WIKILANG и WIKIPEDIANOBOT, какие числа вы видите?


| Topic          | Bytes/sec produced | Bytes/sec consumed |
|----------------|--------------------|--------------------|
| wikilang       | 726                | -                  |
| wikipedianobot | 1347               | 1347               |

#### В KSQL CLI получите текущую статистику вашего стрима: describe extended wikilang; Приложите полный ответ на предыдущий запрос к ответу на задание.

```
Name                 : WIKILANG
Type                 : STREAM
Key field            : 
Key format           : STRING
Timestamp field      : Not set - using <ROWTIME>
Value format         : AVRO
Kafka topic          : WIKILANG (partitions: 2, replication: 2)
 Field     | Type                      
---------------------------------------
 ROWTIME   | BIGINT           (system) 
 ROWKEY    | VARCHAR(STRING)  (system) 
 CREATEDAT | BIGINT                    
 CHANNEL   | VARCHAR(STRING)           
 USERNAME  | VARCHAR(STRING)           
 WIKIPAGE  | VARCHAR(STRING)           
 DIFFURL   | VARCHAR(STRING)           
---------------------------------------
Queries that write from this STREAM
-----------------------------------
CSAS_WIKILANG_7 : CREATE STREAM WIKILANG WITH (KAFKA_TOPIC='WIKILANG', PARTITIONS=2, REPLICAS=2) AS SELECT
  WIKIPEDIA.CREATEDAT "CREATEDAT",
  WIKIPEDIA.CHANNEL "CHANNEL",
  WIKIPEDIA.USERNAME "USERNAME",
  WIKIPEDIA.WIKIPAGE "WIKIPAGE",
  WIKIPEDIA.DIFFURL "DIFFURL"
FROM WIKIPEDIA WIKIPEDIA
WHERE ((WIKIPEDIA.ISBOT <> true) AND (WIKIPEDIA.CHANNEL <> '#en.wikipedia'))
EMIT CHANGES;
For query topology and execution plan please run: EXPLAIN <QueryId>
Local runtime statistics
------------------------
messages-per-sec:      2.93   total-messages:      2443     last-message: 2020-02-24T16:16:55.605Z
(Statistics of the local KSQL server interaction with the Kafka topic WIKILANG)
```
#### В KSQL CLI получите текущую статистику WIKIPEDIANOBOT: descrbie extended wikipedianobot; Приложите раздел Local runtime statistics к ответу на задание.

```
Local runtime statistics
------------------------
messages-per-sec:      5.13   total-messages:     18919     last-message: 2020-02-24T16:20:10.331Z
(Statistics of the local KSQL server interaction with the Kafka topic WIKIPEDIANOBOT)
```
#### Почему для wikipedianobot интерфейс показывает также consumer-* метрики?

Потому что в start.sh после ksql команд идут:
```bash
echo -e "\nStart consumers for additional topics: WIKIPEDIANOBOT, EN_WIKIPEDIA_GT_1_COUNTS"
${DIR}/consumers/listen_WIKIPEDIANOBOT.sh
${DIR}/consumers/listen_EN_WIKIPEDIA_GT_1_COUNTS.sh
```

## 4
#### Добавьте mapping - запустите скрипт set_elasticsearch_mapping_lang.sh
Написанный мною скрипт

```bash
#!/bin/bash

HEADER="Content-Type: application/json"
DATA=$( cat << EOF
{
    "settings": {
        "number_of_shards": 1
    },
    "mappings": {
        "wikichange": {
            "properties": {
                "CREATEDAT": {
                    "type": "date"
                },
                "CHANNEL": {
                    "type": "keyword"
                },
                "USERNAME": {
                    "type": "keyword"
                },
                "WIKIPAGE": {
                    "type": "keyword"
                },
                "DIFFURL": {
                    "type": "text"
                }
            }
        }
    }
}
EOF
)

curl -XPUT -H "${HEADER}" --data "${DATA}" 'http://localhost:9200/wikilang?pretty'
echo
```

#### Добавьте Kafka Connect - запустите submit_elastic_sink_lang_config.sh

Пришлось написать скрипт видоизменив `submit_elastic_sink_config.sh`:

```bash
#!/bin/bash

HEADER="Content-Type: application/json"
DATA=$( cat << EOF
{
  "name": "elasticsearch-lang-ksql",
  "config": {
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "consumer.interceptor.classes": "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor",
    "topics": "WIKILANG",
    "topic.index.map": "WIKILANG:wikilang",
    "connection.url": "http://elasticsearch:9200",
    "type.name": "wikichange",
    "key.ignore": true,
    "key.converter.schema.registry.url": "https://schemaregistry:8085",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "https://schemaregistry:8085",
    "value.converter.schema.registry.ssl.truststore.location": "/etc/kafka/secrets/kafka.client.truststore.jks",
    "value.converter.schema.registry.ssl.truststore.password": "confluent",
    "value.converter.schema.registry.ssl.keystore.location": "/etc/kafka/secrets/kafka.client.keystore.jks",
    "value.converter.schema.registry.ssl.keystore.password": "confluent",
    "schema.ignore": true
  }
}
EOF
)

docker-compose exec connect curl -X POST -H "${HEADER}" --data "${DATA}" --cert /etc/kafka/secrets/connect.certificate.pem --key /etc/kafka/secrets/connect.key --tlsv1.2 --cacert /etc/kafka/secrets/snakeoil-ca-1.crt https://connect:8083/connectors
```

#### Опишите что делает каждая из этих операций?

`set_elasticsearch_mapping_lang.sh` - маппинг указывает как сохранять поля документа и как их индексировать
`submit_elastic_sink_lang_config.sh` - коннектор забирает документы из указанного топика Кафки
index pattern tells Kibana which Elasticsearch indices contain the data that you want to work with. Once you create an index pattern, you're ready to interactively explore your data in Discover.

#### Нажав маленьку круглую кнопку со стрелкой вверх под отчетом, вы сможете запросить не только таблицу, но и запрос на Query DSL которым он получен.

Request
```
{
  "size": 0,
  "query": {
    "bool": {
      "must": [
        {
          "match_all": {}
        },
        {
          "range": {
            "CREATEDAT": {
              "gte": 1582571958153,
              "lte": 1582572858153,
              "format": "epoch_millis"
            }
          }
        }
      ],
      "must_not": []
    }
  },
  "_source": {
    "excludes": []
  },
  "aggs": {
    "2": {
      "terms": {
        "field": "CHANNEL",
        "size": 10,
        "order": {
          "_count": "desc"
        }
      }
    }
  }
}
```

Response
```
{
  "took": 6,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": 1228,
    "max_score": 0,
    "hits": []
  },
  "aggregations": {
    "2": {
      "doc_count_error_upper_bound": 0,
      "sum_other_doc_count": 2,
      "buckets": [
        {
          "key": "#commons.wikimedia",
          "doc_count": 520
        },
        {
          "key": "#fr.wikipedia",
          "doc_count": 187
        },
        {
          "key": "#de.wikipedia",
          "doc_count": 174
        },
        {
          "key": "#es.wikipedia",
          "doc_count": 89
        },
        {
          "key": "#ru.wikipedia",
          "doc_count": 84
        },
        {
          "key": "#it.wikipedia",
          "doc_count": 68
        },
        {
          "key": "#en.wiktionary",
          "doc_count": 48
        },
        {
          "key": "#uk.wikipedia",
          "doc_count": 29
        },
        {
          "key": "#zh.wikipedia",
          "doc_count": 23
        },
        {
          "key": "#eu.wikipedia",
          "doc_count": 4
        }
      ]
    }
  },
  "status": 200
}
```
