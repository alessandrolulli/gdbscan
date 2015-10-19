package enn.densityBased

import scala.reflect.ClassTag
import knn.graph.INode
import org.apache.spark.rdd.RDD
import knn.graph.NeighborList
import java.util.Random
import knn.graph.IMetric

abstract class ENNNodeManager[TID : ClassTag, T : ClassTag, TN <: INode[TID, T] : ClassTag] extends Serializable
{
    def createNode(nodeId : TID, nodeValue : T) : TN
    
    def getNodeValue(node : TN) : Option[T] = Option.empty
    
    def getNodeValue(nodeId : TID) : Option[T] = Option.empty
    
    def init(data : RDD[(TID, T)])
    
    def initKNN(vertexRDD : RDD[TN], metric : IMetric[TID, T, TN],  _config : ENNConfig) : RDD[(TN, NeighborList[TID, T, TN])]
  
    
    def nextLong( rng : Random, n : Long ) : Long = {
        // error checking and 2^x checking removed for simplicity.
        var bits : Long = 0L
        var value : Long = 0L
        do {
            bits = ( rng.nextLong() << 1 ) >>> 1;
            value = bits % n;
        } while ( bits - value + ( n - 1 ) < 0L );
        
        if(value >= n) nextLong(rng, n)
        else value;
}
}