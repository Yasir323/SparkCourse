from pyspark.sql import (
	SparkSession,
	Row,
	functions as func
)

spark = SparkSession.builder.appName('SparkSQL').getOrCreate()
people = spark.read.option('header', 'true').option('inferSchema', 'true')\
	.csv('fakefriends-header.csv')

print("Here is our inferred schema:")
people = people.select('age', 'friends')

print("Let's come to the real question.")
print("Average number of friends by age.")
people.groupBy('age').avg('friends').show()
people.groupBy('age').avg('friends').sort('age').show()
people.groupBy('age').agg(func.round(func.avg('friends'), 2)).sort('age').show()
people.groupBy('age').agg(func.round(func.avg('friends'), 2).alias('avgFriends')).sort('avgFriends').show()

spark.stop()
