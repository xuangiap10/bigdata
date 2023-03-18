val filename = "airquality.csv"
val cateCol = 5
val numCol = 4
/*
val filename = "mtcars.csv"
val cateCol = 2
val numCol = 1
*/

val csv = sc.textFile(filename) 
val headerAndRows = csv.map(line => line.split(",").map(_.trim))
val header = headerAndRows.first
val population = headerAndRows.filter(_(0) != header(0)).map(p => (p(cateCol), p(numCol).toDouble)).cache()

  
import org.apache.spark.rdd.RDD
def meanVarFunc(rdd: RDD[(String,Double)]): RDD[(String,(Double,Double))] = {
	rdd.groupByKey().mapValues(values => {
	val n = values.size
	val mean = values.sum / n
	val variance = values.map(x => math.pow(x - mean, 2)).sum / n
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
var arrSample:Array[(String, (Double, Double, Int))] = Array.empty[(String, (Double, Double, Int))]
val count = 1000
for(i <- 1 to count){
	//println(i)
	//---------step 5a: take 100% of the sample with replacement.
	val resampledData = sample.sample(true,1)
	//---------step 5b: same as step 3
	val meanVarResample = meanVarFunc(resampledData)
	//---------step 5c: Keep adding the values in some running sum
	arrSample = arrSample ++ meanVarResample.mapValues(v => (v._1,v._2,1)).collect()
}

//-------------------step 6: get the average and display the result
val rddSampleReduce = sc.parallelize(arrSample).reduceByKey((v1,v2) => (v1._1 + v2._1, v1._2 + v2._2, v1._3 + v2._3))
val meanVarSample = rddSampleReduce.map{case (k, (m,v,cnt)) => (k,(m/cnt,v/cnt))}
outputFunc(meanVarSample)
exit