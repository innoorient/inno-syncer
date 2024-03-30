from pyspark.sql import SparkSession
from pyspark import SparkConf

jar_dependencies = [
    "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3",
    "com.amazonaws:aws-java-sdk-bundle:1.11.920",
    "org.apache.hadoop:hadoop-aws:3.2.0",
    "mysql:mysql-connector-java:8.0.27",
    "org.postgresql:postgresql:42.5.4"
]

packages = ",".join(jar_dependencies)
print("packages: {}".format(packages))


def get_spark_session(catalog: str = "spark_catalog", log_level: str = "ERROR") -> SparkSession:
    conf = SparkConf()
    conf.set("spark.jars.packages", packages)
    conf.set("spark.sql.sources.default", "iceberg")
    conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    conf.set(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkSessionCatalog")
    conf.set(f"spark.sql.catalog.{catalog}.type", "hive")
    conf.set("hive.metastore.uris", "thrift://localhost:9083")
    conf.set(
        "spark.sql.extensions", 
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    return spark

def test_init_spark_session():
    spark = get_spark_session()
    spark.sql("SHOW DATABASES;").show()
    spark.stop()
