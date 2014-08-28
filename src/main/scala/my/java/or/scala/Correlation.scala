package my.java.or.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Correlation {

    def main(args: Array[String]) {
        val appName = "World of Warcraft Auction Analyzer"
        val conf = new SparkConf().setAppName(appName)
        val sc = new SparkContext(conf)

        val filePath = "/home/sadeqzad/Desktop/agg.csv"
        val correlation = getCorrelation(sc, filePath)    
        println("Pearson population correlation coefficient between variables 'current price' and '14-day today's projected price': " + correlation)

        sc.stop
    }

    def getCorrelation(sc: SparkContext, filePath: String): Double = {
        val mainRDD = sc.textFile(filePath)
        val splitted = mainRDD.map(line => line.split(","))
        val selected = splitted.map(array => (array(9),array(13)))
        val casted = selected.map(tuple => (tuple._1.toDouble, tuple._2.toDouble))
        val nonZeroArrOfTuples = casted.filter(tuple => tuple._1!=0 && tuple._2!=0)
        corr(nonZeroArrOfTuples)    
    }

    // def corr(arrOfTuples: Array[Tuple2[Double, Double]]): Double = {
    def corr(arrOfTuples: org.apache.spark.rdd.RDD[(Double, Double)]): Double = {

        arrOfTuples.cache()

        val xVector = arrOfTuples.map(tuple => tuple._1)
        val sigmaX = xVector.reduce((a, b) => a+b)

        val yVector = arrOfTuples.map(tuple => tuple._2)
        val sigmaY = yVector.reduce((a, b) => a+b)

        val n = arrOfTuples.count()
        val meanX = sigmaX / n
        val meanY = sigmaY / n

        val squaredXDiffFromMean = xVector.map(num => Math.pow(num - meanX, 2))
        val sumOfSquaredDiffX = squaredXDiffFromMean.reduce((a, b) => a+b)
        val stdVariationX = Math.sqrt(sumOfSquaredDiffX / n)

        val squaredYDiffFromMean = yVector.map(num => Math.pow(num - meanY, 2))
        val sumOfSquaredDiffY = squaredYDiffFromMean.reduce((a, b) => a+b)
        val stdVariationY = Math.sqrt(sumOfSquaredDiffY / n)

        val sigmaXy = arrOfTuples.map(tuple => tuple._1 * tuple._2).reduce((a, b) => a+b)
        val covariance = sigmaXy / n

        val pearsonPopulationCorrelation = covariance / (stdVariationX * stdVariationY)

        pearsonPopulationCorrelation
    }
}