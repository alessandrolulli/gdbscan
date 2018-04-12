package enn.densityBased

import scala.reflect.ClassTag
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import knn.graph.impl.NodeSimple
import org.apache.spark.broadcast.Broadcast

abstract class ENNNodeManagerValueOnMap[TID : ClassTag, T : ClassTag]
    (@transient val sc : SparkContext) 
    extends ENNNodeManager[TID, T, NodeSimple[TID, T]] with Serializable
{
    var values : Broadcast[scala.collection.Map[TID, T]] = sc.broadcast(Map[TID, T]())
    
    override def init(data : RDD[(TID, T)]) =
    {
        values = sc.broadcast(data.collectAsMap())
    }
    
    override def getNodeValue(node : NodeSimple[TID, T]) = getNodeValue(node.getId)
    
    override def getNodeValue(nodeId : TID) : Option[T] = values.value.get(nodeId)
    
    override def createNode(nodeId : TID, nodeValue : T) = 
    {
        new NodeSimple(nodeId)
    }
    
    def createNode(nodeId : TID) : NodeSimple[TID, T] =  
    {
        new NodeSimple(nodeId)
    }
}