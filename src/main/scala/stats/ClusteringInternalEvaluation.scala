/*
 * Copyright (C) 2011-2012 the original author or authors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package stats

import dataset.DatasetLoad
import enn.densityBased.ENNConfig
import knn.graph.INode
import knn.graph.generation.BruteForce
import knn.graph.impl.NodeGeneric
import knn.metric.IMetric
import knn.metric.impl._
import knn.util.{Point2D, PointND, PointNDBoolean, PointNDSparse}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.reflect.ClassTag
import scala.util.Random

object ClusteringInternalEvaluation {

  def separationLoop[TID: ClassTag, T: ClassTag, TN <: INode[TID, T] : ClassTag](
                                                                                  similarity: IMetric[TID, T, TN],
                                                                                  rddCC: RDD[(TID, TID)],
                                                                                  rddSubject: RDD[(TID, TN)],
                                                                                  sc: SparkContext): (Double, Double) = {
    val allData = rddCC.join(rddSubject) // (seed, subject)
      .map(t => (t._2._1, t._2._2))
      .takeSample(false, 10000, System.currentTimeMillis())


    var sum = 0d
    var number = 0
    val random = new Random

    var count = 0
    while (count < allData.length - 1) {
      var countInner = count + 1
      while (countInner < allData.length) {

        if (!allData(count)._1.equals(allData(countInner)._1)) {
          if (random.nextInt(100) == 0) {
            number += 1
            sum += similarity.similarity(allData(count)._2.getValue, allData(countInner)._2.getValue)
          }
        }

        countInner += 1
      }

      count += 1
    }
    println(number)

    (sum / number, 0d)
  }

  def separation[TID: ClassTag, T: ClassTag, TN <: INode[TID, T] : ClassTag](
                                                                              similarity: IMetric[TID, T, TN],
                                                                              rddCC: RDD[(TID, TID)],
                                                                              rddSubject: RDD[(TID, TN)],
                                                                              sc: SparkContext): (Double, Double) = {
    //    val asp = rddCC.join(rddSubject) // (id, (clusterId, subject)

    val repetition = 10
    val result = for (i <- 1 to repetition) yield separationLoop(similarity, rddCC, rddSubject, sc)

    val toReturn = result.reduce((a, b) => (a._1 + b._1, a._2 + b._2))

    (toReturn._1 / repetition, toReturn._2 / repetition)
  }

  def compactness[TID: ClassTag, T: ClassTag, TN <: INode[TID, T] : ClassTag](
                                                                               similarity: IMetric[TID, T, TN],
                                                                               rddCC: RDD[(TID, TID)],
                                                                               rddSubject: RDD[(TID, TN)],
                                                                               sc: SparkContext): (Double, Double) = {
    val asp = rddCC.join(rddSubject) // (id, (clusterId, subject)
    val clusterIdSet = asp.map(t => (t._2._1, 1)).groupByKey.filter(t => t._2.size > 1).map(t => t._1).collect
    val validClusterNumber = clusterIdSet.size

    val useIt = asp.map(z => (z._2._1, (z._2._1, z._1, z._2._2))).cache
    useIt.first

    val random = new Random

    val asp3 = clusterIdSet.map(t => useIt.filter { case (key, value) => key == t }.values)
    val asp3size = asp3.size

    val kkk = sc.parallelize(asp3.map { case (z) => {

      val allData = z.map(u => u._3).takeSample(false, 100000, System.currentTimeMillis())
      val clusterId = z.map(u => u._1).first
      val random = new Random

      var sum = 0d
      var number = 0

      var count = 0
      while (count < allData.length - 1) {
        var countInner = count + 1
        while (countInner < allData.length) {
          if (random.nextInt(100) == 0) {
            number += 1
            sum += similarity.similarity(allData(count).getValue, allData(countInner).getValue)
          }

          countInner += 1
        }

        count += 1
      }

      (clusterId, sum, number)
    }
    }, 64).filter(t => t._3 > 10).cache()

    val compactnessCount = kkk.map(t => t._3).reduce(_ + _).toDouble

    val compactness = kkk.map(t => (t._2 / t._3) * (t._3 / compactnessCount)).reduce(_+_)

    (compactness, 0)
  }

  def computeInternalEvaluation[TID: ClassTag, T: ClassTag, TN <: INode[TID, T] : ClassTag](similarity: IMetric[TID, T, TN],
                                                                                            rddCC: RDD[(TID, TID)],
                                                                                            rddSubject: RDD[(TID, TN)],
                                                                                            sc: SparkContext,
                                                                                            config: ENNConfig) = {
    val separationValue = separation(similarity, rddCC, rddSubject, sc)
    val compactnessValue = compactness(similarity, rddCC, rddSubject, sc)

    (separationValue, compactnessValue)
  }

  def main(args_ : Array[String], sc: SparkContext): Unit = {
    // super ugly code! make it better!
    val config = new ENNConfig(args_, "CLUSTERING_INTERNAL_EVALUATION")
    //    val sc = config.util.getSparkContext();

    val file = sc.textFile(config.property.dataset, config.property.sparkPartition).cache
    val cluster = DatasetLoad.loadCluster(sc.textFile(config.property.outputFile + "_CLUSTERING", config.property.sparkPartition)).cache

    val dataSize = file.count
    val clusteredDataSize = cluster.count
    val clusterInfo = cluster.map(t => (t._2, 1)).reduceByKey(_ + _).cache()
    val clusterNumber = clusterInfo.count
    val clusterMaxSize = clusterInfo.map(t => t._2).max

    // dataset, epsilon, core, sizeData, sizeDataClustered, clusterNumber, clusterMaxSize, separation, separationWeight, compactness, compactnessWeight
    def printOutput(separationValue: (Double, Double), compactnessValue: (Double, Double)) = {
      config.util.io.printData("internalEvaluation.txt",
        config.property.dataset,
        config.epsilon.toString,
        config.property.coreThreshold.toString,
        dataSize.toString,
        clusteredDataSize.toString,
        clusterNumber.toString,
        clusterMaxSize.toString,
        separationValue._1.toString,
        separationValue._2.toString,
        compactnessValue._1.toString,
        compactnessValue._2.toString)
    }

    config.propertyLoad.get("ennType", "String") match {
      case "BagOfWords" | "BagOfWordsMAP" => {
        val vertexRDD = DatasetLoad.loadBagOfWords(file, config.property, config).map(t => (t._1, new NodeGeneric(t._1, t._2)))
        val metric: IMetric[Long, PointNDSparse, NodeGeneric[Long, PointNDSparse]] =
          new CosineSimilarityNDSparse[Long, NodeGeneric[Long, PointNDSparse]]

        val (separationValue, compactnessValue) = computeInternalEvaluation[Long, PointNDSparse, NodeGeneric[Long, PointNDSparse]](metric, cluster, vertexRDD, sc, config)
        printOutput(separationValue, compactnessValue)
      }
      case "Transaction" | "TransactionMAP" => {
        val vertexRDD = DatasetLoad.loadTransactionData(file, config.property).map(t => (t._1, new NodeGeneric(t._1, t._2)))
        val metric: IMetric[Long, java.util.Set[Int], NodeGeneric[Long, java.util.Set[Int]]] =
          new JaccardSimilaritySet[Long, Int, java.util.Set[Int], NodeGeneric[Long, java.util.Set[Int]]]

        val (separationValue, compactnessValue) = computeInternalEvaluation[Long, java.util.Set[Int], NodeGeneric[Long, java.util.Set[Int]]](metric, cluster, vertexRDD, sc, config)
        printOutput(separationValue, compactnessValue)
      }
      case "ImageBinary" | "ImageBinaryMAP" => {
        val vertexRDD = DatasetLoad.loadImageBinary(file, config.property, config).map(t => (t._1, new NodeGeneric(t._1, t._2)))
        val metric: IMetric[Long, PointNDBoolean, NodeGeneric[Long, PointNDBoolean]] =
          new SimpsonScore[Long, NodeGeneric[Long, PointNDBoolean]]

        val (separationValue, compactnessValue) = computeInternalEvaluation[Long, PointNDBoolean, NodeGeneric[Long, PointNDBoolean]](metric, cluster, vertexRDD, sc, config)
        printOutput(separationValue, compactnessValue)
      }
      case "Household" | "HouseholdMAP" => {
        val vertexRDD = DatasetLoad.loadHousehold(file, config.property).map(t => (t._1, new NodeGeneric(t._1, t._2)))
        val metric: IMetric[Long, PointND, NodeGeneric[Long, PointND]] =
          new EuclidianSimilarityND[Long, PointND, NodeGeneric[Long, PointND]]

        val (separationValue, compactnessValue) = computeInternalEvaluation[Long, PointND, NodeGeneric[Long, PointND]](metric, cluster, vertexRDD, sc, config)
        printOutput(separationValue, compactnessValue)
      }
      case "Point2D" | "Point2DMAP" => {
        val vertexRDD = DatasetLoad.loadPoint2D(file, config.property, config).map(t => (t._1.toLong, t._2)).map(t => (t._1, new NodeGeneric(t._1, t._2)))
        val metric = new EuclidianDistance2D[Long, NodeGeneric[Long, Point2D]]

        val (separationValue, compactnessValue) = computeInternalEvaluation[Long, Point2D, NodeGeneric[Long, Point2D]](metric, cluster, vertexRDD, sc, config)
        printOutput(separationValue, compactnessValue)
      }
      case "String" | "StringMAP" => {
        val vertexRDD = DatasetLoad.loadStringData(file, config.property, config).map(t => (t._1.toLong, new NodeGeneric(t._1.toLong, t._2)))
        val metric = new JaroWinkler[Long, NodeGeneric[Long, String]]

        val (separationValue, compactnessValue) = computeInternalEvaluation[Long, String, NodeGeneric[Long, String]](metric, cluster, vertexRDD, sc, config)
        printOutput(separationValue, compactnessValue)
      }
    }

    //    sc.stop
  }
}