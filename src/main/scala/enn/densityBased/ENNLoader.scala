package enn.densityBased

import java.util.HashSet

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import knn.graph.INode
import knn.graph.impl.NodeGeneric
import knn.graph.impl.NodeSimple
import knn.metric.IMetric
import knn.metric.impl.EuclidianDistance2D
import knn.metric.impl.EuclidianDistanceND
import knn.metric.impl.JaccardSimilaritySet
import knn.metric.impl.JaroWinkler
import knn.util.Point2D
import knn.util.PointND
import util.CCProperties
import util.CCPropertiesImmutable
import util.CCUtil

/**
 * @author alemare
 */
class ENNLoader( args_ : Array[String] ) extends Serializable
{
    val config = new ENNConfig(args_)

    @transient
    val sc = config.util.getSparkContext();
    val printer = new ENNPrintToFile(config)
    
    def loadAndStart  =
    {
        val file = sc.textFile( config.property.dataset, config.property.sparkPartition )
        
            config.propertyLoad.get( "ennType", "String" ) match {
                case "String" =>
                {
                    val vertexRDD = loadStringData( file , config.property)
                    val metric = new JaroWinkler[Long, NodeGeneric[Long, String]]
                    val nodeManager = new ENNNodeManagerValueOnNodeLong[String](sc)
                    nodeManager.init(vertexRDD.map(t => (t._1.toLong, t._2)))
                    
                    val ennRunner = getENNRunnerLongID[String, NodeGeneric[Long, String]](nodeManager)
                    
                    ennRunner.run(metric, vertexRDD.map(t => nodeManager.createNode(t._1.toLong, t._2)))
                }
                case "Point2D" =>
                {
                    val vertexRDD = loadPoint2D( file , config.property)
                    val metric = new EuclidianDistance2D[Long, NodeSimple[Long, Point2D]]
//                    val nodeManager = new ENNNodeManagerValueOnMapLong[Point2D](sc)
                    val nodeManager = new ENNNodeManagerValueOnMapLong[Point2D](sc)
                    nodeManager.init(vertexRDD.map(t => (t._1.toLong, t._2)))
                    
                    val ennRunner = getENNRunnerLongID[Point2D, NodeSimple[Long, Point2D]](nodeManager)
                    
                    ennRunner.run(metric, vertexRDD.map(t => nodeManager.createNode(t._1.toLong, t._2)))
                }
                case "Transaction" =>
                {
                    val vertexRDD = loadTransactionData( file , config.property)
                    val metric : IMetric[Long, java.util.Set[Int], NodeGeneric[Long, java.util.Set[Int]]] = 
                      new JaccardSimilaritySet[Long, Int, java.util.Set[Int], NodeGeneric[Long, java.util.Set[Int]]]
                    val nodeManager = new ENNNodeManagerValueOnNodeLong[java.util.Set[Int]](sc)
                    nodeManager.init(vertexRDD)
                    
                    val ennRunner = getENNRunnerLongID[java.util.Set[Int], NodeGeneric[Long, java.util.Set[Int]]](nodeManager)
                    
                    ennRunner.run(metric, vertexRDD.map(t => nodeManager.createNode(t._1, t._2)))
                }
                case "Household" =>
                {
                    val vertexRDD = loadHousehold( file , config.property)
                    val metric : IMetric[Long, PointND, NodeGeneric[Long, PointND]] = 
                      new EuclidianDistanceND[Long, PointND, NodeGeneric[Long, PointND]]
                    val nodeManager = new ENNNodeManagerValueOnNodeLong[PointND](sc)
                    nodeManager.init(vertexRDD)
                    
                    val ennRunner = getENNRunnerLongID[PointND, NodeGeneric[Long, PointND]](nodeManager)
                    
                    ennRunner.run(metric, vertexRDD.map(t => nodeManager.createNode(t._1, t._2)))
                }
            }
        
        sc.stop
    }
    
    def getENNRunnerLongID[T : ClassTag, TN <: INode[Long, T] : ClassTag](nodeManager : ENNNodeManager[Long, T, TN]) =
    {
        new ENNRunnerLongID[T, TN](printer, config, nodeManager, sc)
    }
    
    def loadHousehold( data : RDD[String], property : CCPropertiesImmutable ) : RDD[( Long, PointND )] =
    {
        val toReturnEdgeList : RDD[( Long, PointND )] = data.map( line =>
            {
                val splitted = line.split( ";" )
//                val splitted = line.split( property.separator )
                if ( splitted.size >= 2 ) {
                    try {
                        ( splitted( 0 ).toLong, new PointND(splitted.slice(3, 10).map(t => t.toDouble).toArray) )
                    } catch {
                        case e : Exception => ( -1L, PointND.NOT_VALID )
                    }
                } else {
                    ( -1L, PointND.NOT_VALID )
                }
            } )

        toReturnEdgeList.filter( t => t._2.size() > 0 )
    }
    
    def loadTransactionData( data : RDD[String], property : CCPropertiesImmutable ) : RDD[( Long, java.util.Set[Int] )] =
    {
        val toReturnEdgeList : RDD[( Long, java.util.Set[Int] )] = data.map( line =>
            {
                val splitted = line.split( ";" )
//                val splitted = line.split( property.separator )
                if ( splitted.size >= 2 ) {
                    try {
                        val set : java.util.Set[Int] = new java.util.HashSet
                        val elSet = splitted(2).split(" ").map(_.toInt).toSet
                        set.addAll(elSet)
                        ( splitted( 0 ).toLong, set )
                    } catch {
                        case e : Exception => ( -1L, new HashSet() )
                    }
                } else {
                    ( -1L, new HashSet() )
                }
            } )

        toReturnEdgeList.filter( t => !t._2.isEmpty )
    }

    def loadPoint2D( data : RDD[String], property : CCPropertiesImmutable ) : RDD[( String, Point2D )] =
    {
        val toReturnEdgeList : RDD[( String, Point2D )] = data.map( line =>
            {
                val splitted = line.split( property.separator )
                if ( /*splitted.size >= 3 &&*/ !splitted( 0 ).trim.isEmpty ) {
                    try {
                        ( splitted( 0 ), new Point2D( splitted( config.columnDataA ).toDouble, splitted( config.columnDataB ).toDouble ) )
                    } catch {
                        case e : Exception => ( "EMPTY", Point2D.NOT_VALID )
                    }
                } else {
                    ( "EMPTY", Point2D.NOT_VALID )
                }
            } )

        toReturnEdgeList.filter( t => !t._1.equals( "EMPTY" ) )
    }
    
    def loadStringData(data : RDD[String], property : CCPropertiesImmutable ) : RDD[(String, String)] =
    {
        val toReturnEdgeList : RDD[(String, String)] = data.map(line =>
            {
                val splitted = line.split(property.separator)
                if (/*splitted.size >= 1 &&*/ !splitted(0).trim.isEmpty) {
                    try {
                        (splitted( 0 ), splitted( config.columnDataA ))
                    } catch {
                        case e : Exception => ("EMPTY","EMPTY")
                    }
                } else {
                    ("EMPTY","EMPTY")
                }
            })

        toReturnEdgeList.filter(t => !t._1.equals("EMPTY"))
    }
}