from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import (
	StructType,
	StructField,
	StringType,
	IntegerType,
	FloatType
)

spark = SparkSession.builder.appName('CustomerSpends').getOrCreate()
schema = StructType([
	StructField('customerID', IntegerType(), True),
	StructField('itemID', IntegerType(), True),
	StructField('amount', FloatType(), True),
])

# Read the file as a dataframe
df = spark.read.schema(schema).csv('customer-orders.csv')
df.printSchema()

spends = df.select('customerID', 'amount')
total_spends = spends.groupBy('customerID').agg(func.round(func.sum('amount'), 2).alias('totalSpends')).sort('totalSpends')
total_spends.show(total_spends.count())

spark.stop()
