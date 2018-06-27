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

import java.util.Random

import knn.graph.{INode, NeighborList}
import knn.metric.IMetric
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

class ENNRunnerLongID[T: ClassTag, N <: INode[Long, T] : ClassTag](_printer: ENNPrintToFile,
                                                                   _config: ENNConfig,
                                                                   _nodeManager: ENNNodeManager[Long, T, N],
                                                                   @transient val _sc: SparkContext) extends Serializable {
  val DEFAULT_STORAGE_LEVEL = StorageLevel.MEMORY_AND_DISK

  def run(metric: IMetric[Long, T, N], vertexRDD: RDD[N]): Unit = {
    vertexRDD.persist(DEFAULT_STORAGE_LEVEL)
    val enn = new ENNScala[Long, T, N](_sc,
      metric,
      _config)


    val initKNN = initializeKNN(metric, vertexRDD, _nodeManager).persist(DEFAULT_STORAGE_LEVEL)
    val initENN = enn.initializeENN(vertexRDD).persist(DEFAULT_STORAGE_LEVEL)

    initKNN.setName("STARTING KNN GRAPH")
    initENN.setName("STARTING ENN GRAPH")

    initKNN.count
    initENN.count
    vertexRDD.unpersist()

    val graph = enn.computeGraph(initKNN,
      initENN,
      _printer,
      0,
      _nodeManager)

    _printer.printENN[Long, T, N](graph, -1, true)
    graph.unpersist()
  }

  def initializeKNN(metric: IMetric[Long, T, N], vertexRDD: RDD[N], nodeManager: ENNNodeManager[Long, T, N]): RDD[(N, NeighborList[Long, T, N])] = {
    nodeManager.initKNN(vertexRDD, metric, _config)
  }
}