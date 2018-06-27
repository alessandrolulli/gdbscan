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

import knn.graph.{Neighbor, NeighborList}
import knn.graph.impl.NodeGeneric
import knn.metric.IMetric
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

class ENNNodeManagerValueOnNodeLong[T : ClassTag](@transient val scHere : SparkContext)  extends ENNNodeManagerValueOnNode[Long, T](scHere) 
{
     def initKNN(vertexRDD : RDD[NodeGeneric[Long, T]], metric : IMetric[Long, T, NodeGeneric[Long, T]],  _config : ENNConfig) 
        : RDD[(NodeGeneric[Long, T], NeighborList[Long, T, NodeGeneric[Long, T]])] =
    {
        val count = vertexRDD.count
        val neighborListFactory = metric.getNeighborListFactory
        
        val randomNeighbors = vertexRDD.map(t => (t, _config.initPolicy.generateKId(t.getId, count)))
        
        val invertedNeighbours = randomNeighbors.flatMap(t => (t._2.map(u => (u, t._1)))).groupByKey
        
        val joinedValues = invertedNeighbours.join(vertexRDD.map(t => (t.getId, t.getValue)))
        
        joinedValues.flatMap(t => (t._2._1.map(u => (u, new Neighbor[Long, T, NodeGeneric[Long, T]](
                                   createNode(t._1, t._2._2)
                                   , Double.MaxValue))))).groupByKey.map(t => (t._1,{
            val l = neighborListFactory.create(_config.k)
            l.addAll(t._2)
            
            l
        }))
    }
}