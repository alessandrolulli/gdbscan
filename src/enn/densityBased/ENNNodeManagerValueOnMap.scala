package enn.densityBased

import scala.reflect.ClassTag
import org.apache.spark.SparkContext
import knn.graph.INode
import knn.graph.NodeSimple
import scala.collection.mutable.Map
import org.apache.spark.rdd.RDD

abstract class ENNNodeManagerValueOnMap[TID : ClassTag, T : ClassTag]
    (@transient val sc : SparkContext) 
    extends ENNNodeManager[TID, T, NodeSimple[TID, T]] with Serializable
{
    var values = sc.broadcast(Map[TID, T]())
    
    override def init(data : RDD[(TID, T)]) =
    {
        values = sc.broadcast(collection.mutable.Map() ++ data.collect.toMap)
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