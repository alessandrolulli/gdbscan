package enn.densityBased

import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import knn.graph.impl.NodeGeneric

abstract class ENNNodeManagerValueOnNode[TID : ClassTag, T : ClassTag]
    (@transient val sc : SparkContext) 
    extends ENNNodeManager[TID, T, NodeGeneric[TID, T]] with Serializable
{
    override def getNodeValue(node : NodeGeneric[TID, T]) = Option.apply(node.getValue) 
    
    override def createNode(nodeId : TID, nodeValue : T) = new NodeGeneric(nodeId, nodeValue)
    
    override def init(data : RDD[(TID, T)]) = {}
}