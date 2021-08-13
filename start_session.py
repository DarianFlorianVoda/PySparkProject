from pyspark.sql import SparkSession
def session():
    # local[*] = All cores of CPU
    # .config("spark.jars", "C:\spark\spark-3.1.2-bin-hadoop3.2\jars\mysql-connector-java-8.0.17.jar")
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('PySpark_Tutorial') \
        .getOrCreate()
    return spark