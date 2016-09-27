package enn.densityBased

import java.util.HashSet
import scala.reflect.ClassTag
import org.apache.spark.SparkContext
import knn.graph.INode
import knn.graph.Node
import org.apache.spark.rdd.RDD
import knn.graph.NodeGeneric

abstract class ENNNodeManagerValueOnNode[TID : ClassTag, T : ClassTag]
    (@transient val sc : SparkContext) 
    extends ENNNodeManager[TID, T, NodeGeneric[TID, T]] with Serializable
{
    override def getNodeValue(node : NodeGeneric[TID, T]) = Option.apply(node.getValue) 
    
    override def createNode(nodeId : TID, nodeValue : T) = new NodeGeneric(nodeId, nodeValue)
    
    override def init(data : RDD[(TID, T)]) = {}
}