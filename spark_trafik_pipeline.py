from pyspark.sql import SparkSession 

from pyspark.sql.functions import from_json, col, current_timestamp 

from pyspark.sql.types import StructType, StructField, StringType 

  

spark = SparkSession.builder \ 

    .appName("TrafikHaberPipeline") \ 

    .config("spark.jars.packages", 

            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") \ 

    .getOrCreate() 

  

df_raw = spark.readStream \ 

    .format("kafka") \ 

    .option("kafka.bootstrap.servers", "localhost:9092") \ 

    .option("subscribe", "trafik_haberleri_raw") \ 

    .option("startingOffsets", "earliest") \ 

    .load() 

  

schema = StructType([ 

    StructField("id",     StringType()), 

    StructField("il",     StringType()), 

    StructField("terim",  StringType()), 

    StructField("baslik", StringType()), 

    StructField("link",   StringType()), 

    StructField("tarih",  StringType()) 

]) 

df_parsed = df_raw.select( 

    from_json(col("value").cast("string"), schema).alias("data") 

).select("data.*") 

  

df_final = df_parsed.withColumn("islem_zamani", current_timestamp()) 

  

def write_to_mongo(batch_df, batch_id): 

    batch_df.write \ 

        .format("mongodb") \ 

        .mode("append") \ 

        .option("database", "trafikdb") \ 

        .option("collection", "haberler") \ 

        .save() 

  

query_mongo = df_final.writeStream \ 

    .foreachBatch(write_to_mongo) \ 

    .outputMode("append") \ 

    .start() 

  

query_lake = df_final.writeStream \ 

    .format("parquet") \ 

    .option("path", "/home/vboxuser/trafik_datalake") \ 

    .option("checkpointLocation", "/home/vboxuser/trafik_checkpoint") \ 

    .outputMode("append") \ 

    .start() 

  

spark.streams.awaitAnyTermination() 