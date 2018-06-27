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

package enn.densityBased

import dataset.DatasetLoad
import knn.graph.INode
import knn.graph.impl.{NodeGeneric, NodeSimple}
import knn.metric.IMetric
import knn.metric.impl._
import knn.util.{Point2D, PointND, PointNDBoolean, PointNDSparse}
import org.apache.spark.SparkContext

import scala.reflect.ClassTag

class ENNLoader(args_ : Array[String]) extends Serializable {
  val config = new ENNConfig(args_)

  @transient
  val sc: SparkContext = config.util.getSparkContext()
  sc.setLogLevel("ERROR")
  val printer = new ENNPrintToFile(config)

  def loadAndStart = {
    val file = sc.textFile(config.property.dataset, config.property.sparkPartition)

    config.propertyLoad.get("ennType", "String") match {
      case "String" => {
        val vertexRDD = DatasetLoad.loadStringData(file, config.property, config)
        val metric = new JaroWinkler[Long, NodeGeneric[Long, String]]
        val nodeManager = new ENNNodeManagerValueOnNodeLong[String](sc)
        nodeManager.init(vertexRDD.map(t => (t._1.toLong, t._2)))

        val ennRunner = getENNRunnerLongID[String, NodeGeneric[Long, String]](nodeManager)

        ennRunner.run(metric, vertexRDD.map(t => nodeManager.createNode(t._1.toLong, t._2)))
      }
      case "StringMAP" => {
        val vertexRDD = DatasetLoad.loadStringData(file, config.property, config)
        val metric = new JaroWinkler[Long, NodeSimple[Long, String]]
        val nodeManager = new ENNNodeManagerValueOnMapLong[String](sc)
        nodeManager.init(vertexRDD.map(t => (t._1.toLong, t._2)))

        val ennRunner = getENNRunnerLongID[String, NodeSimple[Long, String]](nodeManager)

        ennRunner.run(metric, vertexRDD.map(t => nodeManager.createNode(t._1.toLong, t._2)))
      }
      case "Point2D" => {
        val vertexRDD = DatasetLoad.loadPoint2D(file, config.property, config)
        val metric = new EuclidianDistance2D[Long, NodeGeneric[Long, Point2D]]
        //                    val nodeManager = new ENNNodeManagerValueOnMapLong[Point2D](sc)
        val nodeManager = new ENNNodeManagerValueOnNodeLong[Point2D](sc)
        //        nodeManager.init(vertexRDD.map(t => (t._1.toLong, t._2)))

        val ennRunner = getENNRunnerLongID[Point2D, NodeGeneric[Long, Point2D]](nodeManager)

        ennRunner.run(metric, vertexRDD.map(t => nodeManager.createNode(t._1.toLong, t._2)))
      }
      case "Point2DMAP" => {
        val vertexRDD = DatasetLoad.loadPoint2D(file, config.property, config)
        val metric = new EuclidianDistance2D[Long, NodeSimple[Long, Point2D]]
        //                    val nodeManager = new ENNNodeManagerValueOnMapLong[Point2D](sc)
        val nodeManager = new ENNNodeManagerValueOnMapLong[Point2D](sc)
        nodeManager.init(vertexRDD.map(t => (t._1.toLong, t._2)))

        val ennRunner = getENNRunnerLongID[Point2D, NodeSimple[Long, Point2D]](nodeManager)

        ennRunner.run(metric, vertexRDD.map(t => nodeManager.createNode(t._1.toLong, t._2)))
      }
      case "PointND" => {
        val vertexRDD = DatasetLoad.loadPointND(file, config.property, config.dimensionLimit, config)
        val metric = new EuclidianDistanceND[Long, PointND, NodeGeneric[Long, PointND]]
        //                    val nodeManager = new ENNNodeManagerValueOnMapLong[Point2D](sc)
        val nodeManager = new ENNNodeManagerValueOnNodeLong[PointND](sc)
        nodeManager.init(vertexRDD.map(t => (t._1.toLong, t._2)))

        val ennRunner = getENNRunnerLongID[PointND, NodeGeneric[Long, PointND]](nodeManager)

        ennRunner.run(metric, vertexRDD.map(t => nodeManager.createNode(t._1.toLong, t._2)))
      }
      case "PointNDMAP" => {
        val vertexRDD = DatasetLoad.loadPointND(file, config.property, config.dimensionLimit, config)
        val metric = new EuclidianDistanceND[Long, PointND, NodeSimple[Long, PointND]]
        val nodeManager = new ENNNodeManagerValueOnMapLong[PointND](sc)
        //val nodeManager = new ENNNodeManagerValueOnNodeLong[PointND](sc)
        nodeManager.init(vertexRDD.map(t => (t._1.toLong, t._2)))

        val ennRunner = getENNRunnerLongID[PointND, NodeSimple[Long, PointND]](nodeManager)

        ennRunner.run(metric, vertexRDD.map(t => nodeManager.createNode(t._1.toLong, t._2)))
      }
      case "Transaction" => {
        val vertexRDD = DatasetLoad.loadTransactionData(file, config.property)
        val metric: IMetric[Long, java.util.Set[Int], NodeGeneric[Long, java.util.Set[Int]]] =
          new JaccardSimilaritySet[Long, Int, java.util.Set[Int], NodeGeneric[Long, java.util.Set[Int]]]
        val nodeManager = new ENNNodeManagerValueOnNodeLong[java.util.Set[Int]](sc)
        nodeManager.init(vertexRDD)

        val ennRunner = getENNRunnerLongID[java.util.Set[Int], NodeGeneric[Long, java.util.Set[Int]]](nodeManager)

        ennRunner.run(metric, vertexRDD.map(t => nodeManager.createNode(t._1, t._2)))
      }
      case "TransactionMAP" => {
        val vertexRDD = DatasetLoad.loadTransactionData(file, config.property)
        val metric: IMetric[Long, java.util.Set[Int], NodeSimple[Long, java.util.Set[Int]]] =
          new JaccardSimilaritySet[Long, Int, java.util.Set[Int], NodeSimple[Long, java.util.Set[Int]]]
        val nodeManager = new ENNNodeManagerValueOnMapLong[java.util.Set[Int]](sc)
        nodeManager.init(vertexRDD)

        val ennRunner = getENNRunnerLongID[java.util.Set[Int], NodeSimple[Long, java.util.Set[Int]]](nodeManager)

        ennRunner.run(metric, vertexRDD.map(t => nodeManager.createNode(t._1, t._2)))
      }
      case "Household" => {
        val vertexRDD = DatasetLoad.loadHousehold(file, config.property)
        val metric: IMetric[Long, PointND, NodeGeneric[Long, PointND]] =
          new EuclidianSimilarityND[Long, PointND, NodeGeneric[Long, PointND]]
        val nodeManager = new ENNNodeManagerValueOnNodeLong[PointND](sc)
        nodeManager.init(vertexRDD)

        val ennRunner = getENNRunnerLongID[PointND, NodeGeneric[Long, PointND]](nodeManager)

        ennRunner.run(metric, vertexRDD.map(t => nodeManager.createNode(t._1, t._2)))
      }
      case "HouseholdMAP" => {
        val vertexRDD = DatasetLoad.loadHousehold(file, config.property)
        val metric: IMetric[Long, PointND, NodeSimple[Long, PointND]] =
          new EuclidianSimilarityND[Long, PointND, NodeSimple[Long, PointND]]
        val nodeManager = new ENNNodeManagerValueOnMapLong[PointND](sc)
        nodeManager.init(vertexRDD)

        val ennRunner = getENNRunnerLongID[PointND, NodeSimple[Long, PointND]](nodeManager)

        ennRunner.run(metric, vertexRDD.map(t => nodeManager.createNode(t._1, t._2)))
      }
      case "BagOfWords" => {
        val vertexRDD = DatasetLoad.loadBagOfWords(file, config.property, config)
        val metric: IMetric[Long, PointNDSparse, NodeGeneric[Long, PointNDSparse]] =
          new CosineSimilarityNDSparse[Long, NodeGeneric[Long, PointNDSparse]]
        val nodeManager = new ENNNodeManagerValueOnNodeLong[PointNDSparse](sc)
        nodeManager.init(vertexRDD)

        val ennRunner = getENNRunnerLongID[PointNDSparse, NodeGeneric[Long, PointNDSparse]](nodeManager)

        ennRunner.run(metric, vertexRDD.map(t => nodeManager.createNode(t._1, t._2)))
      }
      case "BagOfWordsMAP" => {
        val vertexRDD = DatasetLoad.loadBagOfWords(file, config.property, config)
        val metric: IMetric[Long, PointNDSparse, NodeSimple[Long, PointNDSparse]] =
          new CosineSimilarityNDSparse[Long, NodeSimple[Long, PointNDSparse]]
        val nodeManager = new ENNNodeManagerValueOnMapLong[PointNDSparse](sc)
        nodeManager.init(vertexRDD)

        val ennRunner = getENNRunnerLongID[PointNDSparse, NodeSimple[Long, PointNDSparse]](nodeManager)

        ennRunner.run(metric, vertexRDD.map(t => nodeManager.createNode(t._1, t._2)))
      }
      case "ImageBinary" => {
        val vertexRDD = DatasetLoad.loadImageBinary(file, config.property, config)
        val metric: IMetric[Long, PointNDBoolean, NodeGeneric[Long, PointNDBoolean]] =
          new SimpsonScore[Long, NodeGeneric[Long, PointNDBoolean]]
        val nodeManager = new ENNNodeManagerValueOnNodeLong[PointNDBoolean](sc)
        nodeManager.init(vertexRDD)

        val ennRunner = getENNRunnerLongID[PointNDBoolean, NodeGeneric[Long, PointNDBoolean]](nodeManager)

        ennRunner.run(metric, vertexRDD.map(t => nodeManager.createNode(t._1, t._2)))
      }
      case "ImageBinaryMAP" => {
        val vertexRDD = DatasetLoad.loadImageBinary(file, config.property, config)
        val t = vertexRDD.count
        val metric: IMetric[Long, PointNDBoolean, NodeSimple[Long, PointNDBoolean]] =
          new SimpsonScore[Long, NodeSimple[Long, PointNDBoolean]]
        val nodeManager = new ENNNodeManagerValueOnMapLong[PointNDBoolean](sc)
        nodeManager.init(vertexRDD)

        val ennRunner = getENNRunnerLongID[PointNDBoolean, NodeSimple[Long, PointNDBoolean]](nodeManager)

        ennRunner.run(metric, vertexRDD.map(t => nodeManager.createNode(t._1, t._2)))
      }
    }
  }

  def stop : Unit = {
    sc.stop
  }

  def getENNRunnerLongID[T: ClassTag, N <: INode[Long, T] : ClassTag](nodeManager: ENNNodeManager[Long, T, N]) : ENNRunnerLongID[T, N] = {
    new ENNRunnerLongID[T, N](printer, config, nodeManager, sc)
  }

}