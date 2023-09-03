import pyspark
import sys

if len(sys.argv) != 3:
  raise Exception("Exactly 2 arguments are required: <inputUri> <outputUri>")
  
sc = pyspark.SparkContext()

rdd = sc.textFile(sys.argv[1]).persist()

def computeContribs(neighbors, rank):
    for neighbor in neighbors:
        yield (neighbor, rank/len(neighbors))
        
linksRDD= rdd.map(lambda x:tuple(x.split())).map(lambda x:(x[0],[x[1]])).reduceByKey(lambda x, y: x+y).collect()

linksRDD = sc.parallelize(linksRDD)
ranksRDD = linksRDD.map(lambda x:(x[0],1.0)).collect()

ranksRDD = sc.parallelize(ranksRDD)
for iteration in range(2): 
  contribs=linksRDD.join(ranksRDD).map(lambda x:x[1]).flatMap(lambda x: computeContribs(x[0],x[1]))
  ranksRDD = contribs.reduceByKey(lambda x, y: x + y).mapValues(lambda x: 0.15 + 0.85 * x)

ranksRDD.saveAsTextFile(sys.argv[2])
