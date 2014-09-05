package org.apache.spark.mllib.clustering

import org.scalatest.FunSuite

import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.mllib.linalg.Vectors

import org.apache.spark.mllib.clustering



/**
 * Created by alex on 8/12/14.
 */
class FuzzyCMeansSuite extends FunSuite with LocalSparkContext{

  import KMeans.{RANDOM, K_MEANS_PARALLEL}


  test("single cluster"){
    println("Start the test!")
//    val data = sc.parallelize(Array(
//      Vectors.dense(1.0, 2.0, 6.0),
//      Vectors.dense(1.0, 3.0, 0.0),
//      Vectors.dense(1.0, 4.0, 6.0)
//    ))
//
//    println(data)
//    val center = Vectors.dense(1.0, 3.0, 4.0)

    // No matter how many runs or iterations we use, we should get one cluster,
    // centered at the mean of the points

    //var model = KMeans.train(data, k=1, maxIterations=1)


    //var model = FuzzyCMeans.train(data, k=1, maxIterations =5)

    val smallData = Array(
      Vectors.dense(1.0, 2.0, 6.0),
      Vectors.dense(1.0, 3.0, 0.0),
      Vectors.dense(1.0, 4.0, 6.0)
    )
    val data = sc.parallelize((1 to 100).flatMap(_ => smallData), 4)
    val center = Vectors.dense(1.0, 3.0, 4.0)
    var model = FuzzyCMeans.train(data, k=1, maxIterations=1, runs=5)



    //var model = FuzzyCMeans.train(data, runs = 5, k=3,maxIterations = 10)



    //assert(model.clusterCenters.head === center)

  }

}
