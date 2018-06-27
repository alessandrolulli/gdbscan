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

import java.util
import java.util.{HashSet, Random}

import knn.graph.{INode, Neighbor, NeighborList, NeighborListFactory}
import knn.metric.IMetric
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

class ENNScala[I: ClassTag, T: ClassTag, N <: INode[I, T] : ClassTag](@transient val sc: SparkContext,
                                                                           val _metric: IMetric[I, T, N],
                                                                           val _config: ENNConfig)
  extends Serializable {
  val DEFAULT_STORAGE_LEVEL = StorageLevel.MEMORY_AND_DISK

  /**
    * each node calculate all pairs similarity between neighbors and
    * send to each neighbor the list of all the neighbors paired with the similarities
    */
  def neighborSimilarityInnerLoop(comparison: LongAccumulator,
                                  node: N,
                                  list: Iterable[N],
                                  nodeManager: ENNNodeManager[I, T, N],
                                  nodeExcluded: Broadcast[Set[I]],
                                  neighborListFactory: NeighborListFactory[I, T, N]): NeighborList[I, T, N] = {
    val nl = neighborListFactory.create(_config.k);

    val tValue = nodeManager.getNodeValue(node)
    var internalComparison = 0

    if (tValue.isDefined && !nodeExcluded.value.contains(node.getId)) {
      list.foreach(u => {
        val uValue = nodeManager.getNodeValue(u)
        if (u != node && uValue.isDefined && !nodeExcluded.value.contains(u.getId)) {
          nl.addNoContains(new Neighbor[I, T, N](u, _metric.compare(uValue.get, tValue.get)))
          internalComparison += 1
        }
      })
    }
    comparison.add(internalComparison)

    nl
  }

  def neighborSimilarityLoopAll(comparison: LongAccumulator,
                                nodes: Set[N],
                                nodeManager: ENNNodeManager[I, T, N],
                                nodeExcluded: Broadcast[Set[I]],
                                neighborListFactory: NeighborListFactory[I, T, N]): Iterable[(N, NeighborList[I, T, N])] = {
    val toReturn: Map[I, (N, NeighborList[I, T, N])] = nodes.map(t => (t.getId, (t, neighborListFactory.create(_config.k)))).toMap
    var internalComparison = 0

    nodes.foreach{case(node) => {
      val nodeValue = nodeManager.getNodeValue(node)

      if (nodeValue.isDefined && !nodeExcluded.value.contains(node.getId)) {
        nodes.foreach{case(neighbor) => {
          if (neighbor.getId.hashCode < node.getId.hashCode && !nodeExcluded.value.contains(neighbor.getId)) {
            val neighborValue = nodeManager.getNodeValue(neighbor)
            if (neighborValue.isDefined) {
              val similarity = _metric.compare(neighborValue.get, nodeValue.get)
              internalComparison += 1
              toReturn(neighbor.getId)._2.addNoContains(new Neighbor[I, T, N](node, similarity))
              toReturn(node.getId)._2.addNoContains(new Neighbor[I, T, N](neighbor, similarity))
            }
          }
        }}
      }

    }}

    comparison.add(internalComparison)
    toReturn.map(t => (t._2._1, t._2._2))
  }

  private def samplingNodes(self_ : N, list_ : Iterable[N]) = {
    if (_config.sampling > 0) {
      list_.toSet.take((_config.k * _config.sampling) - 1) ++ Set(self_)
    }
    else {
      list_.toSet ++ Set(self_)
    }
  }

  def computeGraph(graphInput_ : RDD[(N, NeighborList[I, T, N])],
                   eNNgraphInput_ : RDD[(N, HashSet[I])],
                   printer_ : ENNPrintToFile,
                   iterSum_ : Int,
                   nodeManager_ : ENNNodeManager[I, T, N]): RDD[(N, HashSet[I])] = {
    val nodeManagerBC = sc.broadcast(nodeManager_)
    val neighborListFactoryBC = sc.broadcast(_metric.getNeighborListFactory())

    var graphENN = eNNgraphInput_
    var graphKNN: RDD[(N, NeighborList[I, T, N])] = graphInput_

    val totalNodes = graphInput_.count

    var nodeExcluded = sc.broadcast(Set[I]())

    var iteration = 1
    var earlyTermination = false

    val comparison = sc.longAccumulator("comparison")

    def computingNode(id_ : I, iterationNumber_ : Int, splitComputationNumber_ : Int): Boolean = {
      id_.hashCode() % splitComputationNumber_ == iterationNumber_ % splitComputationNumber_
    }

    while (iteration <= _config.maxIterations && !earlyTermination) {
      val timeBegin = System.currentTimeMillis()
      val iterationNumber = iteration + iterSum_;

      /**
        * some statistics and data to alternate the computation
        * these are not mandatory. If alternating computation is not required can be avoided
        */
      val activeNodes = graphKNN.count

      val splitComputationNumber = Math.max(Math.ceil(activeNodes / _config.maxComputingNodes.toDouble).toInt, 1)

      /**
        * from directed graph to undirected
        */
      val graphKNNiterationStart = graphKNN

      val exploded_graph =
        graphKNN.flatMap { case (node, neighborsList) =>
          neighborsList.flatMap { case (neighbor) => {
            if (computingNode(neighbor.node.getId, iterationNumber, splitComputationNumber)) {
              Array((node, neighbor.node), (neighbor.node, node))
            } else {
              Array((node, neighbor.node))
            }
          }
          }
        }.groupByKey // i obtained equals performance

      /**
        * some statistics for plotting
        */
      val computingNodes = if (_config.performance) 0 else exploded_graph.filter(tuple => tuple._1.getId.hashCode() % splitComputationNumber == iterationNumber % splitComputationNumber).count

      graphKNN = exploded_graph.flatMap { case (node, neighbors) => {
        /**
          * alternating computation
          * in iteration t only the nodes having id % t equals to 0 perform all pairs similarity of the neighbors
          */
        if (computingNode(node.getId, iterationNumber, splitComputationNumber)) {
          val nodes = samplingNodes(node, neighbors)
          neighborSimilarityLoopAll(comparison, nodes, nodeManagerBC.value, nodeExcluded, neighborListFactoryBC.value)
        }
        else {
          Array[(N, NeighborList[I, T, N])]((node, neighborSimilarityInnerLoop(comparison, node, neighbors, nodeManagerBC.value, nodeExcluded, neighborListFactoryBC.value)))
        }
      }
      }.filter(t => !t._2.isEmpty())

      /**
        * for each node we gather the better kMax neighbors (in reduceByKey)
        * after (in map) we generate an rdd where for each node we keep:
        * 1) the best k neighbors -> for the KNN graph
        * 2) all the neighbors valid for ENN -> for the ENN graph
        */
      val graphBeforeFilter: RDD[(N, (NeighborList[I, T, N], Iterable[I]))] = graphKNN.reduceByKey((a, b) => {
        val neighborKNN = neighborListFactoryBC.value.create(Math.max(_config.k, _config.kMax))

        neighborKNN.addAll(a)
        neighborKNN.addAll(b)

        neighborKNN
      }).map(t => (t._1, ( {
        t._2.convertWithSize(_config.k)
      }, {
        t._2.flatMap(p => if (_metric.isValid(p, _config.epsilon)) Some(p.node.getId) else None) //.toSet
      })))

      /**
        * for what concern the KNN construction we keep only the top k neighbors and we discard the ENN neighbors
        */
      graphBeforeFilter.setName("GRAPH-BEFORE-FILTER: " + iteration)
      graphBeforeFilter.persist(DEFAULT_STORAGE_LEVEL)

      val previousGraphKNN = graphKNN
      graphKNN = graphBeforeFilter.map(t => (t._1, t._2._1))

      /**
        * for what concern the ENN construction we keep only the ENN neighbors and we discard the KNN neighbors
        * than we join the previous ENN with the current ENN neighbors
        * it returns also a boolean meaning that the vertex must be excluded or not from the computation
        */
      val previousGraphENN = graphENN

      val graphENNPlusExclusion: RDD[(N, (HashSet[I], Boolean))] =
        graphENN.leftOuterJoin(graphBeforeFilter.map(t => (t._1, t._2._2))).map(arg => {
          if (arg._2._1.size() >= _config.kMax) {
            (arg._1, (arg._2._1, arg._2._2.isDefined))
          }
          else {
            if (arg._2._2.isDefined) {
              val nl = arg._2._1

              nl.addAll(arg._2._2.get)

              (arg._1, (nl, nl.size() >= _config.kMax))
            }
            else {
              (arg._1, (arg._2._1, arg._2._1.size() >= _config.kMax))
            }
          }
        })

      nodeExcluded = sc.broadcast(graphENNPlusExclusion.filter(t => t._2._2).map(t => t._1.getId).collect().toSet)

      val nodeExludedNumber = nodeExcluded.value.size

      graphENN = graphENNPlusExclusion.map(t => (t._1, t._2._1)).persist(DEFAULT_STORAGE_LEVEL)
      graphENN.setName("GRAPH-ENN: " + iteration)
      graphENN.count()
      previousGraphENN.unpersist()
      graphENNPlusExclusion.unpersist()
      graphKNN.setName("GRAPH-KNN: " + iteration)
      graphKNN.persist(DEFAULT_STORAGE_LEVEL).count
      graphKNNiterationStart.unpersist()
      previousGraphKNN.unpersist()
      graphBeforeFilter.unpersist()

      val timeEnd = System.currentTimeMillis()

      if (!_config.performance) {
        printer_.printENN[I, T, N](graphENN, iterationNumber)
      }
      //        printer_.printKNNDistribution(iteration, graphKNN)

      printer_.printTime(iterationNumber, timeEnd - timeBegin, activeNodes, computingNodes, nodeExludedNumber, comparison.value)

      /* early termination mechanism */
      if ((activeNodes - nodeExludedNumber <= 0) || activeNodes < (totalNodes * _config.terminationActiveNodes) && nodeExludedNumber < (totalNodes * _config.terminationRemovedNodes)) {
        earlyTermination = true
      }
      iteration = iteration + 1
    }

    //      printer_.printENNDistribution[TID, T, TN](iteration, graphENN)

    graphENN
  }

  def initializeENN(nodes_ : RDD[N]) : RDD[(N, util.HashSet[I])] = {
    nodes_.map(t => (t, new HashSet[I]()))
  }
}