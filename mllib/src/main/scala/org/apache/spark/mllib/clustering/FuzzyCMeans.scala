package org.apache.spark.mllib.clustering

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import breeze.linalg.{DenseVector => BDV, Vector => BV, norm => breezeNorm}

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
    private var epsilon: Double)
//extends KMeans(k,maxIterations, runs, initializationMode, initializationSteps, epsilon)
  extends KMeans
{

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


  def initMatrix() = {
    println("initializing the matrix")
    for (i <- 0 to numOfRows) {
      for (j <- 0 to numOfCols) {
        //should cast to Float!!!
        matrix(i)(j) = 0
      }
    }
  }

  def printMatrix() = {
    println("printing the matrix")
    for(i<-0 to this.numOfCols){
      for(j<-0 to numOfCols){
        println("ff")
      }
    }
    println("done printing")
  }

}


//object MembershipMatrix{
//  //def initMembershipMatrix(numOfRows: Int, numOfCols: Int) = MembershipMatrix(numOfRows, numOfCols)
//  def initMembershipMatrix(rows: Int, cols: Int) : MembershipMatrix = MembershipMatrix(rows, cols)
//}
//










