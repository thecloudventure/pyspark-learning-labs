from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("TotalByCustomer")
sc = SparkContext(conf=conf)

def extractCustomerPricePairs(line):
    fields = line.split(",")
    return(int(fields[0]), float(fields[2]))

inputFile = sc.textFile("../../data/customer-orders.csv")
mappedInput = inputFile.map(extractCustomerPricePairs)
totalByCustomer = mappedInput.reduceByKey(lambda x,y: x + y)

#Sort by Amount Spent
flippedDataSet = totalByCustomer.map(lambda x:(x[1],x[0]))
totalByCustomerSorted = flippedDataSet.sortByKey()

results = totalByCustomerSorted.collect()

for result in results:
    print(result)