from pyspark import SparkConf, SparkContext
# import string
# import re

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input_ = sc.textFile("Book")
words = input_.flatMap(lambda x: x.split())
# Remove punctuations
# regex = re.compile('[%s]' % re.escape(string.punctuation))
# words = words_with_punc.map(lambda s: regex.sub('', s))
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
