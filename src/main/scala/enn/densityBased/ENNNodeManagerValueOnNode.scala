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

import knn.graph.impl.NodeGeneric
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

abstract class ENNNodeManagerValueOnNode[TID : ClassTag, T : ClassTag]
    (@transient val sc : SparkContext) 
    extends ENNNodeManager[TID, T, NodeGeneric[TID, T]] with Serializable
{
    override def getNodeValue(node : NodeGeneric[TID, T]) = Option.apply(node.getValue) 
    
    override def createNode(nodeId : TID, nodeValue : T) = new NodeGeneric(nodeId, nodeValue)
    
    override def init(data : RDD[(TID, T)]) = {}
}