from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('Total Customers')

sc = SparkContext(conf = conf)

lines = sc.textFile('file:///D:/Code/SparkCourse/customer-orders.csv')

transactions = lines.map(lambda x: (int(x.split(',')[0]), float(x.split(',')[2])))

# total_by_customer_id = transactions.reduceByKey(lambda x, y: x + y)

total_by_customer_id_sorted = transactions.reduceByKey(lambda x, y: x + y).map(lambda x: (x[1], x[0])).sortByKey()

results = total_by_customer_id_sorted.collect()

for result in results:
    print(str(result[1]) + " => " + str(round(result[0], 2)))
