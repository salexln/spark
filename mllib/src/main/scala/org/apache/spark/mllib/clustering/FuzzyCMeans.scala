package org.apache.spark.mllib.clustering

import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD
import breeze.linalg.{DenseVector => BDV, Vector => BV, norm => breezeNorm, *}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

//import scala.collection.mutable.ArrayBuffer
//
//import breeze.linalg.{DenseVector => BDV, Vector => BV, norm => breezeNorm}
//
//import org.apache.spark.annotation.Experimental
//import org.apache.spark.Logging
//import org.apache.spark.SparkContext._
//import org.apache.spark.mllib.linalg.{Vector, Vectors}
//import org.apache.spark.mllib.util.MLUtils
//import org.apache.spark.rdd.RDD
//import org.apache.spark.util.random.XORShiftRandom


/**
* Created by alex on 8/12/14.
*/

/**
 * Fuzzy C Means algorithm psuedo - code:
 * (taken from
 *  - http://upetd.up.ac.za/thesis/available/etd-10172011-211435/unrestricted/01chapters1-2.pdf)
 *  - http://home.deib.polimi.it/matteucc/Clustering/tutorial_html/cmeans.html
 *
 * 1. Randomly initialize C clusters
 * 2. Initialize membership matrix
 * 3. Repeat
 *  3.1 Recalculate the centroid of each cluster using:
 *      c_j = (SUM_i (u_i_j * x_i) ) / (SUM_i(u_i_j))
 *  3.2 Update the membership matrix U with U' using:
 *      u_i_j = 1 / (SUM_k ( ( ||x_i - c_j||)/ (||x_i - c_k||) ) pow (2/(m-1)))
 *  3.3 Until
 *      MAX_i_k{ ||u_i_k  - u'_i_k || } < epsilon

 */
class FuzzyCMeans(
    private var k: Int,
    private var maxIterations: Int,
    private var runs: Int,
    private var initializationMode: String,
    private var initializationSteps: Int,
    private var epsilon: Double
                   )
//extends KMeans(k,maxIterations, runs, initializationMode, initializationSteps, epsilon)
  extends KMeans
{
  val membershipMatrix : MembershipMatrix = null

  //override def run(data: RDD[ Vector]): KMeansModel = {
  override  def run(data: RDD[Vector]): KMeansModel = {
    // Compute squared norms and cache them.
    val norms = data.map(v => breezeNorm(v.toBreeze, 2.0))
    norms.persist()
    val breezeData = data.map(_.toBreeze).zip(norms).map { case (v, norm) =>
      new BreezeVectorWithNorm(v, norm)
    }
    val model = runBreeze(breezeData)
    //val mode = null
    norms.unpersist()
    model
  }


  override def updateInternalData() = {
    updateMatrix()
  }


  def updateMatrix() = {
    println("updating matrix!!!")
  }

  override def initCenters(data: RDD[BreezeVectorWithNorm]) :
  Array[Array[BreezeVectorWithNorm]] = {

    //init the membership matrix with random floats
    membershipMatrix.initRandomMatrix()

    //init the centers of the cluster
    val centers = super.initCenters(data)
    centers
  }

  /**
   * calculate the centers according to:
   *
   * x_i - the ith of d-dimensional measured data
   * u_i_j - the degree of membership (taken from the matrix)
   * c_j - the d-dimension center of the cluster
   *
   * this method updates the centers according to the following formula
   * c_i = (SUM_i(u_i_j * x_i) ) / (SUM_i (u_i_j) )
   */
  override def calculateCenters(data: RDD[BreezeVectorWithNorm]) : KMeansModel  = {
    //super.calculateCenters(data)
    val sc = data.sparkContext
    val active = Array.fill(runs)(true)
    val costs = Array.fill(runs)(0.0)

    var activeRuns = new ArrayBuffer[Int] ++ (0 until runs)
    var iteration = 0

    val iterationStartTime = System.nanoTime()

    // Execute Dunn & Bezdek algorithm from Fuzzy clustering
    var notConverged :Boolean = true


    while(notConverged && iteration < maxIterations){
      //data.mapPartitions{ points =>

      //get the data array:
      var dataArr: Array[BreezeVectorWithNorm] = data.toArray()

      // new center calculation:
      // c_j = (SUM_i(u_i_j * x_i) ) / (SUM_i (u_i_j) )
      for(j <- 0 until membershipMatrix.getColsNum()){

        // calculate the SUM_i(u_i_j * x_i)
        var columnSum :Float = 0
        for(i <- 0 until membershipMatrix.getRowsNum()){
          columnSum += membershipMatrix.getValue(i, j)
        }

        var totalSum: Float = 0
        // calculate the SUM_i(u_i_j * x_i)
        for(i <-0 until membershipMatrix.getRowsNum()){
          totalSum += membershipMatrix.getValue(i,j) //*dataArr(i)
        }

        var total = (totalSum / columnSum).toDouble

        var totalArr = new Array[Double](0)
        totalArr(0) = total

        //var total: Double = 0
        //var newCenter = total



        val newCenter = new BreezeVectorWithNorm(totalArr)
//        var count = 10
//        var sum : Double = 5
//        sum /= count.toDouble
//        var vec : BV[Double]
//        vec(0) = sum
//        val newCenter2 = new BreezeVectorWithNorm(vec)

        //val newCenter = new BreezeVectorWithNorm(total)
        //var newCenter = totalSum / columnSum

        // update the center array:
        // ALEX - should we update (run)(j) like in KMeans.scala?
        centers(iteration)(j) = newCenter

      }





//        (0 until membershipMatrix.getColsNum()).foreach{ j =>
//          // new center calculation:
//          // c_j = (SUM_i(u_i_j * x_i) ) / (SUM_i (u_i_j) )
//


          // calculate the SUM_i(u_i_j * x_i)


        //}
      //}


        
      }


      //while (iteration < maxIterations && !activeRuns.isEmpty) {
//      type WeightedPoint = (BV[Double], Long)
//      def mergeContribs(p1: WeightedPoint, p2: WeightedPoint): WeightedPoint = {
//        (p1._1 += p2._1, p1._2 + p2._2)
//      }
//
//      val activeCenters = activeRuns.map(r => centers(r)).toArray
//      val costAccums = activeRuns.map(_ => sc.accumulator(0.0))
    //}
    //new KMeansModel(centers(bestRun).map(c => Vectors.fromBreeze(c.vector)))
    new KMeansModel(null)
  }
}



object FuzzyCMeans {
  // Initialization mode names
  val RANDOM = "random"
  val K_MEANS_PARALLEL = "k-means||"


  def train(
             data: RDD[Vector],
             k: Int,
             maxIterations: Int,
             runs: Int,
             initializationMode: String,
             initSteps: Int,
             epsilon: Double
             ): KMeansModel = {

    //new FuzzyCMeans().setK(k).setMaxInterations(maxIterations).setRun(runs).setInitializationSteps(initSteps).initRelationMatrix()
    //new FuzzyCMeansModel(k,maxIterations,runs,initSteps).run(data)


    //val matrix = MembershipMatrix.initMembershipMatrix(3,5)

    //init the membership matrix: it has k rows and the number of columns is as the size of the data

    var matrix =new MembershipMatrix(k,data.partitions.length)

    //set the rows and cols
    //TODO need to find out different way to do it

    matrix.setRowsNum(k)
    matrix.setColsNum(data.partitions.length)


    //matrix.initMatrix()
    matrix.printMatrix()

    val fcm = new FuzzyCMeans(k,maxIterations,runs,initializationMode,initSteps,epsilon)
    fcm.run(data)
//      .setK(k)
//      .setMaxInterations(maxIterations)
//      .setRun(runs)
//      .setInitializationMode(initializationMode)
//      .setInitializationSteps(initSteps)
//      .setEpsilon(epsilon)
//      .run(data)
  }

  def train(
      data: RDD[Vector],
      k: Int,
      maxIterations: Int) : KMeansModel = {
        train(data,k, maxIterations,1, K_MEANS_PARALLEL,10, 1e-4)
  }

  def train(
     data: RDD[Vector],
     k: Int,
     maxIterations: Int,
     runs: Int): KMeansModel = {
      train(data, k, maxIterations, runs, K_MEANS_PARALLEL, 10, 1e-4)
  }
}


class MembershipMatrix(
  private var numOfRows : Int,
  private var numOfCols : Int)
{
  private val matrix  = Array.ofDim[Float](numOfRows, numOfCols)
  //ivate var membershipMatrix = Array.ofDim[Float](numOfRows, numOfCols)
  //private var membershipMatrix = Array.ofDim[Float](2,3)

  def getRowsNum() = this.numOfRows
  def setRowsNum(n:Int) = this.numOfRows = n
  def getColsNum() = this.numOfCols
  def setColsNum(n:Int) = this.numOfCols = n
  def setValue(i: Int, j: Int, value:Float) = {
    matrix(i)(j) = value
  }

  def getValue(row: Int, col: Int ) : Float = this.matrix(row)(col)

  def initRandomMatrix() = {
    /**
     * randomly init the membership matrix:
     * the sum of each row should be 1 - this is the probability of each data point to belong to each cluster
     */
    var random = new Random()

    for(i <- 0 until getColsNum()-1){
      var total: Float = 1;
      for(j <- 0 until getRowsNum()-1){
        if( j == getRowsNum() -1){
          //if this is the last one
          matrix(i)(j) = total
        }
        else{
          var temp = random.nextFloat()
          matrix(i)(j) = temp
          total = total - temp
        }
      }
    }
  }

  def printMatrix() = {
    println("printing the matrix")
//    for(i<-0 to this.numOfCols){
//      for(j<-0 to numOfCols){
//        println("ff")
//      }
//    }
    println("done printing")
  }

}


//object MembershipMatrix{
//  //def initMembershipMatrix(numOfRows: Int, numOfCols: Int) = MembershipMatrix(numOfRows, numOfCols)
//  def initMembershipMatrix(rows: Int, cols: Int) : MembershipMatrix = MembershipMatrix(rows, cols)
//}
//










