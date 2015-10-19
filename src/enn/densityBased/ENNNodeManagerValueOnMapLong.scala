package enn.densityBased

import scala.reflect.ClassTag
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import knn.graph.NodeSimple
import knn.graph.NeighborList
import java.util.Random
import knn.graph.IMetric
import knn.graph.Neighbor

class ENNNodeManagerValueOnMapLong[T : ClassTag](@transient val scHere : SparkContext)  extends ENNNodeManagerValueOnMap[Long, T](scHere){
  
    def initKNN(vertexRDD : RDD[NodeSimple[Long, T]], metric : IMetric[Long, T, NodeSimple[Long, T]],  _config : ENNConfig) 
        : RDD[(NodeSimple[Long, T], NeighborList[Long, T, NodeSimple[Long, T]])] =
    {
        val count = vertexRDD.count
        val neighborListFactory = metric.getNeighborListFactory
        
        vertexRDD.map(t => (t,
            {
               val rand = new Random
               val neighborKNN = neighborListFactory.create(_config.k)
               (1 to _config.k) map (_  => 
                       {
                           val id = nextLong(rand, count)
                       
                           neighborKNN.add(new Neighbor[Long, T, NodeSimple[Long, T]](
                                   createNode(id)
                                   , Double.MaxValue))
                       }                        
               )
                       
               neighborKNN
            }))
    }
}