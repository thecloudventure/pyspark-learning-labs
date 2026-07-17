from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MaxTempreatures")
sc = SparkContext(conf=conf)

def parseLines(line):
    fields = line.split(",")
    stationId = fields[0]
    entryType = fields[2]
    tempreature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return(stationId, entryType, tempreature)

lines = sc.textFile("../../data/1800.csv")
parsedLines = lines.map(parseLines)
maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
stationTemps = maxTemps.map(lambda x: (x[0], x[2]))
maxTemps = stationTemps.reduceByKey(lambda x,y: max(x,y))
results = maxTemps.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))


