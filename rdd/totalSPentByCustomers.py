from pyspark import SparkConf, SparkContext


def parse_line(line):
	row = line.split(',')
	idx = int(row[0])
	price = float(row[2])
	return (idx, price)


conf = SparkConf().setMaster('local').setAppName('CustomerSpends')
sc = SparkContext(conf=conf)

lines = sc.textFile('customer-orders.csv')
rdd = lines.map(parse_line)
total_spends = rdd.reduceByKey(lambda x, y: x + y)
# Flip and sort
total_spends_sorted = total_spends.map(lambda x: (x[1], x[0])).sortByKey()
results = total_spends_sorted.collect()

for result in results:
	print(f"{str(result[1]).zfill(2)} -> {result[0]:.2f}")
