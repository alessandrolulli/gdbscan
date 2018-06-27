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

import knn.graph.impl.NodeSimple
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

abstract class ENNNodeManagerValueOnMap[I: ClassTag, T: ClassTag]
(@transient val sc: SparkContext)
  extends ENNNodeManager[I, T, NodeSimple[I, T]] with Serializable {
  var values: scala.collection.Map[I, T] = Map[I, T]()

  override def init(data: RDD[(I, T)]): Unit = {
    values = data.collectAsMap()
  }

  override def getNodeValue(node: NodeSimple[I, T]): Option[T] = getNodeValue(node.getId)

  override def getNodeValue(nodeId: I): Option[T] = values.get(nodeId)

  override def createNode(nodeId: I, nodeValue: T): NodeSimple[I, T] = {
    new NodeSimple(nodeId)
  }

  def createNode(nodeId: I): NodeSimple[I, T] = {
    new NodeSimple(nodeId)
  }
}