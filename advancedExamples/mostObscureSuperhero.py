from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

names = spark.read.schema(schema).option("sep", " ").csv("Marvel-names")

lines = spark.read.text("Marvel-graph")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.min("connections").alias("connections"))
    
mostObscure = connections.sort(func.col("connections").desc())

mostObscureNames = names.filter(func.col("id") == mostObscure[0]).select("name").first()

mostObscureNames.show()

spark.stop()
