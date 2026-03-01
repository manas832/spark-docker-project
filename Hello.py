# docker run -it --rm -v "C:\Users\manas\OneDrive\Desktop\spark-docker-project:/opt/spark/work-dir" apache/spark:latest /opt/spark/bin/spark-submit /opt/spark/work-dir/Hello.py


from pyspark.sql import SparkSession

# Create Spark Session
spark = SparkSession.builder \
    .appName("SimpleApp") \
    .getOrCreate()


df = spark.read.csv("/opt/spark/work-dir/", header=True, inferSchema=True)
df.createOrReplaceTempView("orders")
# Show
query ="select * from orders"
spark.sql(query)



df.write.mode("overwrite").parquet("s3a://manas-spark-demo-12345/output/")


# Stop Spark
spark.stop()

