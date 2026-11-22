"""
Speed layer (Lambda): Structured Streaming on events.user -> Redis user affinity + hot tags.
"""
from __future__ import annotations

import json
import os

import redis
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType


REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")


schema = StructType(
    [
        StructField("event_id", StringType()),
        StructField("ts", StringType()),
        StructField("type", StringType()),
        StructField("user_id", StringType()),
        StructField("video_id", StringType()),
        StructField("watch_time", DoubleType()),
        StructField("weight", DoubleType()),
        StructField("tags", ArrayType(StringType())),
        StructField("category", StringType()),
    ]
)


def write_partition(rows) -> None:
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    for row in rows:
        user_id = row.user_id
        tags = list(row.tags or [])
        if row.category:
            tags.append(row.category)
        weight = float(row.weight or 1.0)
        pipe = r.pipeline()
        for tag in tags:
            if not tag:
                continue
            t = tag.lower()
            pipe.hincrbyfloat(f"user:affinity:{user_id}", t, weight)
            pipe.zincrby("metrics:hot_tags", abs(weight), t)
        pipe.expire(f"user:affinity:{user_id}", 86400)
        pipe.hincrby("metrics:speed_windows", "updates", 1)
        pipe.execute()


def main() -> None:
    spark = (
        SparkSession.builder.appName("recsys-speed-stream")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "events.user")
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
        .select("e.*")
        .withColumn("event_time", F.to_timestamp("ts"))
        .withWatermark("event_time", "2 minutes")
    )

    windowed = (
        parsed.groupBy(F.window("event_time", "1 minute"), "user_id", F.explode_outer("tags").alias("tag"))
        .agg(F.sum("weight").alias("weight"))
        .filter(F.col("tag").isNotNull())
    )

    def foreach_batch(df, _epoch) -> None:
        # flatten to affinity updates
        rows = df.select(
            "user_id",
            F.array(F.col("tag")).alias("tags"),
            "weight",
            F.lit(None).cast("string").alias("category"),
        ).collect()
        write_partition(rows)

    query = (
        windowed.writeStream.outputMode("update")
        .foreachBatch(foreach_batch)
        .option("checkpointLocation", "/tmp/spark-speed-checkpoint")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
