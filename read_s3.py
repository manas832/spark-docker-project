#docker run -it --rm -v "C:\Users\manas\OneDrive\Desktop\spark-docker-project:/opt/spark/work-dir" apache/spark:latest /opt/spark/bin/spark-submit /opt/spark/work-dir/read_from_s3


from pyspark.sql import SparkSession

# Create Spark Session
spark = SparkSession.builder \
    .appName("SimpleApp") \
    .getOrCreate()


df = spark.read.parquet("s3a://manas-spark-demo-12345/output/")

df.show()
spark.stop();

'''
#docker run -it --rm `
-e AWS_ACCESS_KEY_ID= `
-e AWS_SECRET_ACCESS_KEY= `
-e AWS_REGION=ap-south-1 `
-v "C:\Users\manas\OneDrive\Desktop\spark-docker-project:/opt/spark/work-dir" `
apache/spark:3.5.1 `
/opt/spark/bin/spark-submit `
--packages org.apache.hadoop:hadoop-aws:3.3.4 `
--conf spark.jars.ivy=/tmp/.ivy `
--conf spark.hadoop.fs.s3a.connection.timeout=60000 `
--conf spark.hadoop.fs.s3a.connection.establish.timeout=60000 `
--conf spark.hadoop.fs.s3a.socket.timeout=60000 `
/opt/spark/work-dir/read_s3.py
'''