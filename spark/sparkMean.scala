/*
val csv = sc.textFile("mtcars.csv") 
val headerAndRows = csv.map(line => line.split(",").map(_.trim))
val header = headerAndRows.first
val population = headerAndRows.filter(_(0) != header(0)).map(p => (p(2), p(1).toDouble)).cache()
*/

val csv = sc.textFile("airquality.csv") 
val headerAndRows = csv.map(line => line.split(",").map(_.trim))
val header = headerAndRows.first
val population = headerAndRows.filter(_(0) != header(0)).map(p => (p(5), p(4).toDouble)).cache()

  
import org.apache.spark.rdd.RDD
def meanVarFunc(rdd: RDD[(String,Double)]): RDD[(String,(Double,Double))] = {
	rdd.groupByKey().mapValues(values => {
	val n = values.size
	val mean = values.sum / n
	val variance = values.map(x => math.pow(x - mean, 2)).sum / (n-1)
	(mean, variance)
	})
}

import org.apache.spark.sql.functions._
case class Stat (Category: String, Mean: Double, Variance: Double)

def outputFunc(rdd: RDD[(String,(Double,Double))]): Unit = {
	val rddDS = rdd.sortByKey().map(p => Stat(p._1,p._2._1,p._2._2)).toDS()
	rddDS.show()
}

//-------------------step 3: Compute the mean and variance for each category 
println("----population mean and variance-----")
val meanVarPop = meanVarFunc(population)
outputFunc(meanVarPop)

//-------------------step 4: take 25% of the population without replacement
val sample = population.sample(false,0.25)

//-------------------step 5: do 100 times
var rddReSample: RDD[(String, (Double, Double))] = sc.emptyRDD[(String, (Double, Double))]
val count = 1000
for(i <- 1 to count){
	//println(i)
	//---------step 5a: take 100% of the sample with replacement.
	val resampledData = sample.sample(true,1)
	//---------step 5b: same as step 3
	val meanVarResample = meanVarFunc(resampledData)
	//---------step 5c: Keep adding the values in some running sum
	rddReSample = rddReSample ++ meanVarResample
}

//-------------------step 6: get the average and display the result
val rddSample = rddReSample.mapValues(v => (v._1,v._2,1))
val rddSampleReduce = rddSample.reduceByKey((v1,v2) => (v1._1 + v2._1, v1._2 + v2._2, v1._3 + v2._3))

val meanVarSample = rddSampleReduce.map{case (k, (m,v,cnt)) => (k,(m/cnt,v/cnt))}
outputFunc(meanVarSample)



