## Intro to streaming data with Kafka, Spark and AWS Glue

In this scenario, data arriving periodically from the source is sent to Kafka and transferred to Spark from there. In Spark, data is simply processed and written to S3 in parquet format. Finally, the written raw data is written back to S3 as a single file in parquet format with Glue and transferred to Athena as a table.

Detailed blog post: [Stream Veriye Giriş: Kafka, Spark ve AWS Glue ile Küçük Veri İşleme](https://silverstone1903.github.io/posts/2023/09/stream-101/) (Turkish)

----

### Project Pipeline
```mermaid
stateDiagram-v2
direction LR

state Stream_Data{
producer --> Kafka
Kafka --> Spark_Streaming
Spark_Streaming --> S3_raw_data
}

note right of Stream_Data
    Streaming data (Producer-Consumer)
end note

state Offline_Data{
S3_raw_data --> Glue
Glue --> S3_processed_data
S3_processed_data --> Athena
}

note right of Offline_Data
    Offline Data (AWS Layer)
end note

```


### Frameworks & Versions
* Python 3.7.12
* Spark 3.2.4
* Hadoop 2.7.4
* Kafka 3.5.1

### Screenshots
Spark Structured Streaming UI
![](assests/spark_ui.png)

[Kafka Magic](https://www.kafkamagic.com/): Monitoring for Apache Kafka topics
![](assests/kafka_magic.png)

### Resources
* [kafka-python](https://kafka-python.readthedocs.io/en/master/)
* [Introduction to Kafka](https://docs.confluent.io/kafka/introduction.html)



