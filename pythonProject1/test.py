from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
#Task 1
spark = SparkSession.builder.appName("CSV to DataFrame").getOrCreate()

df = spark.read.csv("C:\\Users\\okuma\\Downloads\\OneDrive_1_8-30-2024\\Input_data.csv", header=True, inferSchema=True)
#Task 2
df_out = df.withColumn("Web_TREE", lit("Default_Value"))

df_out.show()

#Task 3
df_out.write.json("./out/json")
#Task 4
df_out.write.parquet("./out/json")