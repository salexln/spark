package org.apache.spark.mllib.clustering

import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD
import breeze.linalg.{DenseVector => BDV, Vector => BV, norm => breezeNorm}

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

    //we need to init the centers and the matrix

    //init the matrix:

    //init the centers:
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
   * c_i = (SUM_i(u_i_j * x_i) ) / (SUM_i (u_i_j) )
   *
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
    val notConverged :Boolean = true
    while(notConverged){

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










