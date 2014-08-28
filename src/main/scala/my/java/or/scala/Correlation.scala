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
      
    val (rddOfTuples, meanX, meanY, stdDevX, stdDevY) = init(sc, filePath, 9, 13)
    
    val correlation = corr(rddOfTuples, stdDevX, stdDevY)    
    println("\nPearson population correlation coefficient between variables 'current price' and '14-day today's projected price': " + correlation)

    val x = 300
    val (yHat, r2) = predictY(x, rddOfTuples, meanX, meanY, stdDevX, stdDevY)       
    println("\nFor x('14-day today's projected price')=" + x + ", we predict y(real price)=" + yHat)
    println("The determinism coefficient of this linear regression (R^2)= " + r2)        
    
    // after being done, stop the spark context
    sc.stop
  }

  /*
   * xCloumnIndex and yCloumnIndex are 1-based indices (not 0-based)
   * and are equal to the number of colons before the desired property + 1
   */
  def init(sc: SparkContext, filePath: String, xColumnIndex: Int, yCloumnIndex: Int): Tuple5[org.apache.spark.rdd.RDD[(Double, Double)], Double, Double, Double, Double] = {
    val mainRDD = sc.textFile(filePath)
    val splitted = mainRDD.map(line => line.split(","))
    val selected = splitted.map(array => (array(xColumnIndex), array(yCloumnIndex)))
    val casted = selected.map(tuple => (tuple._1.toDouble, tuple._2.toDouble))
    val nonZeroRDDOfTuples = casted.filter(tuple => tuple._1 != 0 && tuple._2 != 0)
    val (meanX, meanY) = getMeans(nonZeroRDDOfTuples)
    val (stdDevX, stdDevY) = getStdDev(nonZeroRDDOfTuples, meanX, meanY)
    (nonZeroRDDOfTuples, meanX, meanY, stdDevX, stdDevY)
  }  

  def corr(rddOfTuples: org.apache.spark.rdd.RDD[(Double, Double)], stdDevX: Double, stdDevY: Double): Double = {
    rddOfTuples.cache()
    val n = rddOfTuples.count()
    val sigmaXy = rddOfTuples.map(tuple => tuple._1 * tuple._2).reduce((a, b) => a + b)
    val covariance = sigmaXy / n
    val pearsonPopulationCorrelation = covariance / (stdDevX * stdDevY)
    pearsonPopulationCorrelation
  }
  
  
  /*
   * returns a tuple containing the prediction of the dependent variable and the determinism coefficient (R^2)
   * based on the value of the input (the independent variable)
   * Uses simple linear regression to do so
   * (figures out the coefficients to form the formula of the linear regression)
   */
  def predictY(x: Double, rddOfTuples: org.apache.spark.rdd.RDD[(Double, Double)], meanX: Double, meanY: Double, stdDevX: Double, stdDevY: Double): (Double, Double) = {
    val (b0, b1, r2) = getlinRegrCoefs(rddOfTuples, meanX, meanY, stdDevX, stdDevY)
    val yHat = b0 + (b1 * x)
    (yHat, r2)
  }
  
  /*
   * returns a Tuple3 containing coefficients b0 and b1 
   * (to be used in the simple linear regression formula) 
   * and also the determinism coefficient (R^2)
   */
  def getlinRegrCoefs(rddOfTuples: org.apache.spark.rdd.RDD[(Double, Double)], meanX: Double, meanY: Double, stdDevX: Double, stdDevY: Double): Tuple3[Double, Double, Double] = {
    rddOfTuples.cache()
    val diffFromMeanXY = rddOfTuples.map(tuple => (tuple._1 - meanX) * (tuple._2 - meanY))
    val sigmadiffFromMeanXY = diffFromMeanXY.reduce((a, b) => a+b)

    val squaredXDiffFromMean = rddOfTuples.map(tuple => Math.pow(tuple._1 - meanX, 2))
    val sigmaSquaredXDiff = squaredXDiffFromMean.reduce((a, b) => a+b)
    val b1 = sigmadiffFromMeanXY / sigmaSquaredXDiff
    val b0 = meanY - (b1 * meanX)    
    val n = rddOfTuples.count()
    val R2 = Math.pow(((1/n) * sigmadiffFromMeanXY / (stdDevX * stdDevY)), 2)
    (b0, b1, R2)
  }
    
  
  /*
   * returns standard deviation of variables (vectors) X and Y
   */
  def getStdDev(rddOfTuples: org.apache.spark.rdd.RDD[(Double, Double)], meanX: Double, meanY: Double): Tuple2[Double, Double] = {
	rddOfTuples.cache()	
    val xVector = rddOfTuples.map(tuple => tuple._1)
    val yVector = rddOfTuples.map(tuple => tuple._2)
    val n = rddOfTuples.count()    
    val squaredXDiffFromMean = xVector.map(x => Math.pow(x - meanX, 2))
    val sumOfSquaredDiffX = squaredXDiffFromMean.reduce((a, b) => a + b)
    val stdDevX = Math.sqrt(sumOfSquaredDiffX / n)

    val squaredYDiffFromMean = yVector.map(y => Math.pow(y - meanY, 2))
    val sumOfSquaredDiffY = squaredYDiffFromMean.reduce((a, b) => a + b)
    val stdDevY = Math.sqrt(sumOfSquaredDiffY / n)    
    // return standard deviations as a tuple
    (stdDevX, stdDevY)
  }
  
  /*
   * returns the means of the both vectors (RDDs)
   */
  def getMeans(rddOfTuples: org.apache.spark.rdd.RDD[(Double, Double)]): (Double, Double) = {
    rddOfTuples.cache()	
    val xVector = rddOfTuples.map(tuple => tuple._1)
    val sigmaX = xVector.reduce((a, b) => a + b)
    val yVector = rddOfTuples.map(tuple => tuple._2)
    val sigmaY = yVector.reduce((a, b) => a + b)
    val n = rddOfTuples.count()
    val meanX = sigmaX / n
    val meanY = sigmaY / n    
    (meanX, meanY)
  }
  
}