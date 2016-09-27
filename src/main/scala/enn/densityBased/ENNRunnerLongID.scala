package enn.densityBased

import scala.reflect.ClassTag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import knn.graph.INode
import knn.graph.IMetric
import org.apache.spark.SparkContext
import knn.graph.NeighborList
import knn.graph.NodeSimple
import java.util.Random
import knn.graph.Neighbor
import org.apache.spark.storage.StorageLevel

/**
 * @author alemare
 */
class ENNRunnerLongID[T : ClassTag, TN <: INode[Long, T] : ClassTag] (_printer : ENNPrintToFile,
                                                                      _config : ENNConfig,
                                                                      _nodeManager : ENNNodeManager[Long, T, TN],
                                                                      @transient val _sc : SparkContext) extends Serializable
{
    val DEFAULT_STORAGE_LEVEL = StorageLevel.MEMORY_AND_DISK

    def run(metric : IMetric[Long, T, TN], vertexRDD : RDD[TN]) =
    {
        val enn = new ENNScala[Long, T, TN]( _sc  ,
                metric ,
                _config)


        val initKNN = initializeKNN(metric, vertexRDD, _nodeManager).persist(DEFAULT_STORAGE_LEVEL)
        val initENN = enn.initializeENN(vertexRDD).persist(DEFAULT_STORAGE_LEVEL)

        initKNN.count
        initENN.count
        vertexRDD.unpersist()

        val graph = enn.computeGraph(initKNN,
                                     initENN,
                                     _printer,
                                     0,
                                     _nodeManager)

        _printer.printENN[Long, T,TN](graph, -1, true)
    }

    def initializeKNN(metric : IMetric[Long, T, TN], vertexRDD : RDD[TN], nodeManager : ENNNodeManager[Long, T, TN]): RDD[(TN, NeighborList[Long, T, TN])] =
    {
        nodeManager.initKNN(vertexRDD, metric, _config)

//        val count = vertexRDD.count
//        val neighborListFactory = metric.getNeighborListFactory
//
//        vertexRDD.map(t => (t,
//            {
//               val rand = new Random
//               val neighborKNN = neighborListFactory.create(_config.k)
//               (1 to _config.k) map (_  =>
//                       {
//
//                           val id = nextLong(rand, count)
//                           val value = nodeManager.getNodeValue(id)
//
//                           if(value.isDefined)
//                               neighborKNN.add(new Neighbor[Long, T, TN](
//                                       nodeManager.createNode(id, value.get)
//                                       , Double.MaxValue))
//                       }
//               )
//
//               neighborKNN
//            }))
    }

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