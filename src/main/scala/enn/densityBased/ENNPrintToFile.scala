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

import java.util.HashSet

import knn.graph.{INode, NeighborList}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions.{asScalaSet, collectionAsScalaIterable}

class ENNPrintToFile(config: ENNConfig) extends Serializable {
  var _time = 0L

  def printENN[TID, T, TN <: INode[TID, T]](rdd: RDD[(TN, HashSet[TID])], iteration: Int, force: Boolean = false): Unit = {
    if (force || (config.printStep > 0 && iteration % config.printStep == 0)) {
      val graphScala = rdd.map(t => (t._1.getId, t._2.toList.map(u => (u, 0))))
      val toPrint = graphScala.map(t => t._1 + "\t" + t._2.map(u => u._1 + " " + u._2).mkString(" "))
      //            toPrint.coalesce(1, true).saveAsTextFile(config.property.outputFile+"_iter"+iteration)
      if (force) {
        toPrint.saveAsTextFile(config.property.outputFile)
      } else {
        toPrint.saveAsTextFile(config.property.outputFile + "_iter" + iteration)
      }
    }
  }

  def printTime(iteration: Int, time: Long, totalNode: Long, computingNodes: Double, stoppedNodes: Double, messageNumber: Long) {
    _time = _time + time
    val timeToPrint = _time

    config.util.io.printStatENN(
      iteration,
      timeToPrint,
      config.k,
      config.kMax,
      config.epsilon,
      config.randomRestart,
      config.printStep,
      totalNode,
      computingNodes,
      stoppedNodes,
      config.performance,
      messageNumber,
      config)
  }

  def printKNNDistribution[TID, T, TN <: INode[TID, T]](iteration: Int, rdd: RDD[(TN, NeighborList[TID, T, TN])]) {
    def roundAt(p: Int)(n: Double): Double = {
      val s = math pow(10, p); (math round n * s) / s
    }

    def roundAt2(n: Double) = roundAt(2)(n)

    rdd.flatMap(t => t._2.map(u => (roundAt2(u.similarity), 1)))
      .reduceByKey(_ + _)
      .collect
      .foreach(t => config.util.io.printData("knn_distribution.txt",
        config.property.dataset,
        iteration.toString,
        t._1.toString,
        t._2.toString))

    rdd.map(t => (roundAt2(t._2.getMinSimilarity.similarity), 1))
      .reduceByKey(_ + _)
      .collect
      .foreach(t => config.util.io.printData("knn_distribution_min.txt",
        config.property.dataset,
        iteration.toString,
        t._1.toString,
        t._2.toString))

    rdd.map(t => (roundAt2(t._2.getMaxSimilarity.similarity), 1))
      .reduceByKey(_ + _)
      .collect
      .foreach(t => config.util.io.printData("knn_distribution_max.txt",
        config.property.dataset,
        iteration.toString,
        t._1.toString,
        t._2.toString))
  }

  def printENNDistribution[TID, T, TN <: INode[TID, T]](iteration: Int, rdd: RDD[(TN, HashSet[TID])]) {
    rdd.map(t => (t._2.size, 1))
      .reduceByKey(_ + _)
      .collect
      .foreach(t => config.util.io.printData("enn_distribution.txt",
        config.property.dataset,
        config.epsilon.toString,
        t._1.toString,
        t._2.toString))
  }
}