from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)


lines = sc.textFile('fakefriends.csv')
rdd = lines.map(parseLine)  # (key, value) = (age, numFriends)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
# mapValues leaves the keys untouched, so x here refers to value i.e., numFriends only
# rdd.mapValues(lambda x: (x, 1)) => (age, (numFriends, 1))
# reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) => (age, (groupby(age).sum(numFriends), groupby(age).count()))
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()
for result in results:
    print(result)
