import logging

import pyspark

def main():
    spark = (
        pyspark.sql.SparkSession.builder.master("local[2]")
        .config(
            "spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        )
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config(
            "spark.driver.extraJavaOptions",
            "-Dio.netty.tryReflectionSetAccessible=true",
        )
        .config(
            "spark.executor.extraJavaOptions",
            "-Dio.netty.tryReflectionSetAccessible=true",
        )
        .config("spark.driver.memory", "6g")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", "TODO")
        .config("spark.hadoop.fs.s3a.secret.key", "TODO")
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
        .getOrCreate()
    )
    logging.getLogger("py4j").setLevel(logging.ERROR)
    # "s3a://commoncrawl/cc-index/table/cc-main/warc/"
    df = spark.read.format("parquet").load("s3a://commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2022-33/subset=warc/*.parquet")
    df.createOrReplaceTempView("cc_index")
    df.printSchema()
    print(spark.sql("SELECT url FROM cc_index WHERE content_mime_detected = 'text/vtt' LIMIT 10").toPandas())

    # 

if __name__ == '__main__':
    main()

# schema:
# root                                                                            
#  |-- url_surtkey: string (nullable = true)
#  |-- url: string (nullable = true)
#  |-- url_host_name: string (nullable = true)
#  |-- url_host_tld: string (nullable = true)
#  |-- url_host_2nd_last_part: string (nullable = true)
#  |-- url_host_3rd_last_part: string (nullable = true)
#  |-- url_host_4th_last_part: string (nullable = true)
#  |-- url_host_5th_last_part: string (nullable = true)
#  |-- url_host_registry_suffix: string (nullable = true)
#  |-- url_host_registered_domain: string (nullable = true)
#  |-- url_host_private_suffix: string (nullable = true)
#  |-- url_host_private_domain: string (nullable = true)
#  |-- url_host_name_reversed: string (nullable = true)
#  |-- url_protocol: string (nullable = true)
#  |-- url_port: integer (nullable = true)
#  |-- url_path: string (nullable = true)
#  |-- url_query: string (nullable = true)
#  |-- fetch_time: timestamp (nullable = true)
#  |-- fetch_status: short (nullable = true)
#  |-- fetch_redirect: string (nullable = true)
#  |-- content_digest: string (nullable = true)
#  |-- content_mime_type: string (nullable = true)
#  |-- content_mime_detected: string (nullable = true)
#  |-- content_charset: string (nullable = true)
#  |-- content_languages: string (nullable = true)
#  |-- content_truncated: string (nullable = true)
#  |-- warc_filename: string (nullable = true)
#  |-- warc_record_offset: integer (nullable = true)
#  |-- warc_record_length: integer (nullable = true)
#  |-- warc_segment: string (nullable = true)
