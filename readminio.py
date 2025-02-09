from pyspark.sql import SparkSession
import os

# Chemins des JARs dans le dossier Spark
spark_home = "C:/Spark/jars"
hadoop_aws_jar = os.path.join(spark_home, "hadoop-aws-3.3.4.jar")
aws_sdk_jar = os.path.join(spark_home, "aws-java-sdk-bundle-2.20.66.jar")
aws_sdk_core_jar = os.path.join(spark_home, "aws-java-sdk-core-2.20.66.jar")
aws_sdk_s3_jar = os.path.join(spark_home, "aws-java-sdk-s3-2.20.66.jar")

# Création de la session Spark avec MinIO et les jars nécessaires
spark = SparkSession.builder \
    .appName("MinioTest") \
    .config("spark.jars", f"{hadoop_aws_jar},{aws_sdk_jar},{aws_sdk_core_jar},{aws_sdk_s3_jar}") \
    .getOrCreate()

# Configuration de MinIO
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minio")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minio123")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://127.0.0.1:9000")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")

# Chemin du fichier Parquet sur MinIO
parquet_path = "s3a://warehouse/transactions.parquet"

# Lecture du fichier Parquet
df = spark.read.parquet(parquet_path)

# Affichage des données
df.show()
