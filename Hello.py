#docker run -it --rm -v "C:\Users\manas\OneDrive\Desktop\spark-docker-project:/opt/spark/work-dir" apache/spark:latest /opt/spark/bin/spark-submit /opt/spark/work-dir/Hello.py


from pyspark.sql import SparkSession

# Create Spark Session
spark = SparkSession.builder \
    .appName("SimpleApp") \
    .getOrCreate()

# Create sample DataFrame
data = [("Manas", 25), ("Rahul", 30), ("Amit", 28)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)

# Show data
df.show()

# Stop Spark
spark.stop()