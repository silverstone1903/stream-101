# https://stackoverflow.com/questions/72627688/pyspark-noclassdeffounderror-kafka-common-topicandpartition/72639163#72639163
from pyspark.sql import SparkSession
import cfg

scala_version = "2.12"
spark_version = "3.2.4"
packages = [
    f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}",
    "org.apache.kafka:kafka-clients:3.5.1",
    "org.apache.hadoop:hadoop-aws:2.7.4",
]
spark = (
    SparkSession.builder.master("local[6]")
    .appName("kafka-example")
    .config("spark.jars.packages", ",".join(packages))
    .config("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4")
    .getOrCreate()
)

spark.sparkContext._jsc.hadoopConfiguration().set(
    "fs.s3a.access.key", cfg.config.aws_access_key_id
)
spark.sparkContext._jsc.hadoopConfiguration().set(
    "fs.s3a.secret.key", cfg.config.aws_secret_access_key
)
spark._jsc.hadoopConfiguration().set(
    "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
)
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.eu-central-1.amazonaws.com")
spark._jsc.hadoopConfiguration().set("fs.s3a.acl.default", "BucketOwnerFullControl")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")


from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.streaming import StreamingContext
from pyspark.sql.streaming import DataStreamWriter


df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", cfg.config.bootstrap_servers)
    .option("subscribe", cfg.config.topic)
    .option("startingOffsets", "earliest")  # latest
    .option("failOnDataLoss", "false")
    .load()
)
print("Stream Schema")
df.printSchema()

schema = (
    StructType()
    .add("name", StringType())
    .add("mail", StringType())
    .add("job", StringType())
    .add("address", StringType())
    .add("country", StringType())
    .add("age", IntegerType())
    .add("salary", IntegerType())
    .add("register_date", DateType())
)
text_stream = (
    df.selectExpr("CAST(value as STRING)", "timestamp")
    .select(from_json("value", schema).alias("data"), "timestamp")
    .select("data.*", "timestamp")
)
print("Stream DF Schema")
text_stream.printSchema()

text_stream.writeStream.format("parquet").option("path", cfg.config.data).outputMode(
    "append"
).option("checkpointLocation", cfg.config.checkpoint_location).option(
    "truncate", "false"
).trigger(
    processingTime="60 seconds"
).start().awaitTermination()
