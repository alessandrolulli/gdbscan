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
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

abstract class ENNNodeManager[I: ClassTag, T: ClassTag, N <: INode[I, T] : ClassTag] extends Serializable {
  def createNode(nodeId: I, nodeValue: T): N

  def getNodeValue(node: N): Option[T] = Option.empty

  def getNodeValue(nodeId: I): Option[T] = Option.empty

  def init(data: RDD[(I, T)]): Unit

  def initKNN(vertexRDD: RDD[N], metric: IMetric[I, T, N], _config: ENNConfig): RDD[(N, NeighborList[I, T, N])]

  def nextLong(rng: Random, n: Long): Long = {
    // error checking and 2^x checking removed for simplicity.
    var bits: Long = 0L
    var value: Long = 0L
    do {
      bits = (rng.nextLong() << 1) >>> 1
      value = bits % n
    } while (bits - value + (n - 1) < 0L)

    if (value >= n) {
      nextLong(rng, n)
    } else {
      value
    }
  }

  def value = this
}