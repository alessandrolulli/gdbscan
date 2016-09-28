package enn.densityBased

import java.util.Random

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import knn.graph.Neighbor
import knn.graph.NeighborList
import knn.graph.impl.NodeGeneric
import knn.metric.IMetric

class ENNNodeManagerValueOnNodeLong[T : ClassTag](@transient val scHere : SparkContext)  extends ENNNodeManagerValueOnNode[Long, T](scHere) 
{
     def initKNN(vertexRDD : RDD[NodeGeneric[Long, T]], metric : IMetric[Long, T, NodeGeneric[Long, T]],  _config : ENNConfig) 
        : RDD[(NodeGeneric[Long, T], NeighborList[Long, T, NodeGeneric[Long, T]])] =
    {
        val count = vertexRDD.count
        val neighborListFactory = metric.getNeighborListFactory
        
        val randomNeighbors = vertexRDD.map(t => (t,
        {
           val rand = new Random
           
           (1 to _config.k) map (_  => 
           {
               nextLong(rand, count)
           })
        }))
        
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