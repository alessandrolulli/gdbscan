package enn.densityBased

import java.util.HashSet

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import dataset.DatasetLoad
import knn.graph.INode
import knn.graph.impl.NodeGeneric
import knn.graph.impl.NodeSimple
import knn.metric.IMetric
import knn.metric.impl.CosineSimilarityNDSparse
import knn.metric.impl.EuclidianDistance2D
import knn.metric.impl.EuclidianDistanceND
import knn.metric.impl.JaccardSimilaritySet
import knn.metric.impl.JaroWinkler
import knn.metric.impl.SimpsonScore
import knn.util.Point2D
import knn.util.PointND
import knn.util.PointNDBoolean
import knn.util.PointNDSparse
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
                case "StringMAP" =>
                {
                    val vertexRDD = loadStringData( file , config.property)
                    val metric = new JaroWinkler[Long, NodeSimple[Long, String]]
                    val nodeManager = new ENNNodeManagerValueOnMapLong[String](sc)
                    nodeManager.init(vertexRDD.map(t => (t._1.toLong, t._2)))
                    
                    val ennRunner = getENNRunnerLongID[String, NodeSimple[Long, String]](nodeManager)
                    
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
                case "PointND" =>
                {
                    val vertexRDD = loadPointND( file , config.property, config.dimensionLimit)
                    val metric = new EuclidianDistanceND[Long, PointND, NodeGeneric[Long, PointND]]
//                    val nodeManager = new ENNNodeManagerValueOnMapLong[Point2D](sc)
                    val nodeManager = new ENNNodeManagerValueOnNodeLong[PointND](sc)
                    nodeManager.init(vertexRDD.map(t => (t._1.toLong, t._2)))

                    val ennRunner = getENNRunnerLongID[PointND, NodeGeneric[Long, PointND]](nodeManager)

                    ennRunner.run(metric, vertexRDD.map(t => nodeManager.createNode(t._1.toLong, t._2)))
                }
                case "Transaction" =>
                {
                    val vertexRDD = DatasetLoad.loadTransactionData( file , config.property)
                    val metric : IMetric[Long, java.util.Set[Int], NodeGeneric[Long, java.util.Set[Int]]] = 
                      new JaccardSimilaritySet[Long, Int, java.util.Set[Int], NodeGeneric[Long, java.util.Set[Int]]]
                    val nodeManager = new ENNNodeManagerValueOnNodeLong[java.util.Set[Int]](sc)
                    nodeManager.init(vertexRDD)
                    
                    val ennRunner = getENNRunnerLongID[java.util.Set[Int], NodeGeneric[Long, java.util.Set[Int]]](nodeManager)
                    
                    ennRunner.run(metric, vertexRDD.map(t => nodeManager.createNode(t._1, t._2)))
                }
                case "TransactionMAP" =>
                {
                    val vertexRDD = DatasetLoad.loadTransactionData( file , config.property)
                    val metric : IMetric[Long, java.util.Set[Int], NodeSimple[Long, java.util.Set[Int]]] = 
                      new JaccardSimilaritySet[Long, Int, java.util.Set[Int], NodeSimple[Long, java.util.Set[Int]]]
                    val nodeManager = new ENNNodeManagerValueOnMapLong[java.util.Set[Int]](sc)
                    nodeManager.init(vertexRDD)
                    
                    val ennRunner = getENNRunnerLongID[java.util.Set[Int], NodeSimple[Long, java.util.Set[Int]]](nodeManager)
                    
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
                case "HouseholdMAP" =>
                {
                    val vertexRDD = loadHousehold( file , config.property)
                    val metric : IMetric[Long, PointND, NodeSimple[Long, PointND]] = 
                      new EuclidianDistanceND[Long, PointND, NodeSimple[Long, PointND]]
                    val nodeManager = new ENNNodeManagerValueOnMapLong[PointND](sc)
                    nodeManager.init(vertexRDD)
                    
                    val ennRunner = getENNRunnerLongID[PointND, NodeSimple[Long, PointND]](nodeManager)
                    
                    ennRunner.run(metric, vertexRDD.map(t => nodeManager.createNode(t._1, t._2)))
                }
                case "BagOfWords" =>
                {
                    val vertexRDD = DatasetLoad.loadBagOfWords(file , config.property, config)
                    val metric : IMetric[Long, PointNDSparse, NodeGeneric[Long, PointNDSparse]] = 
                      new CosineSimilarityNDSparse[Long, NodeGeneric[Long, PointNDSparse]]
                    val nodeManager = new ENNNodeManagerValueOnNodeLong[PointNDSparse](sc)
                    nodeManager.init(vertexRDD)
                    
                    val ennRunner = getENNRunnerLongID[PointNDSparse, NodeGeneric[Long, PointNDSparse]](nodeManager)
                    
                    ennRunner.run(metric, vertexRDD.map(t => nodeManager.createNode(t._1, t._2)))
                }
                case "BagOfWordsMAP" =>
                {
                    val vertexRDD = DatasetLoad.loadBagOfWords(file , config.property, config)
                    val metric : IMetric[Long, PointNDSparse, NodeSimple[Long, PointNDSparse]] = 
                      new CosineSimilarityNDSparse[Long, NodeSimple[Long, PointNDSparse]]
                    val nodeManager = new ENNNodeManagerValueOnMapLong[PointNDSparse](sc)
                    nodeManager.init(vertexRDD)
                    
                    val ennRunner = getENNRunnerLongID[PointNDSparse, NodeSimple[Long, PointNDSparse]](nodeManager)
                    
                    ennRunner.run(metric, vertexRDD.map(t => nodeManager.createNode(t._1, t._2)))
                }
                case "ImageBinary" =>
                {
                    val vertexRDD = DatasetLoad.loadImageBinary(file , config.property, config)
                    val metric : IMetric[Long, PointNDBoolean, NodeGeneric[Long, PointNDBoolean]] = 
                      new SimpsonScore[Long, NodeGeneric[Long, PointNDBoolean]]
                    val nodeManager = new ENNNodeManagerValueOnNodeLong[PointNDBoolean](sc)
                    nodeManager.init(vertexRDD)
                    
                    val ennRunner = getENNRunnerLongID[PointNDBoolean, NodeGeneric[Long, PointNDBoolean]](nodeManager)
                    
                    ennRunner.run(metric, vertexRDD.map(t => nodeManager.createNode(t._1, t._2)))
                }
                case "ImageBinaryMAP" =>
                {
                    val vertexRDD = DatasetLoad.loadImageBinary(file , config.property, config)
                    val t = vertexRDD.count
                    val metric : IMetric[Long, PointNDBoolean, NodeSimple[Long, PointNDBoolean]] = 
                      new SimpsonScore[Long, NodeSimple[Long, PointNDBoolean]]
                    val nodeManager = new ENNNodeManagerValueOnMapLong[PointNDBoolean](sc)
                    nodeManager.init(vertexRDD)
                    
                    val ennRunner = getENNRunnerLongID[PointNDBoolean, NodeSimple[Long, PointNDBoolean]](nodeManager)
                    
                    ennRunner.run(metric, vertexRDD.map(t => nodeManager.createNode(t._1, t._2)))
                }
            }
    }
    
    def stop =
    {
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
    
    def loadPointND( data : RDD[String], property : CCPropertiesImmutable, dimensionLimit : Int ) : RDD[( String, PointND )] =
    {
        val toReturnEdgeList : RDD[( String, PointND )] = data.map( line =>
            {
                val splitted = line.split( property.separator )
                if (!splitted( 0 ).trim.isEmpty ) {
                    try {
                        ( splitted( 0 ), new PointND(splitted(config.columnDataA).split(" ").slice(0, dimensionLimit).map(x => x.toDouble) ))
                    } catch {
                        case e : Exception => ( "EMPTY", PointND.NOT_VALID )
                    }
                } else {
                    ( "EMPTY", PointND.NOT_VALID )
                }
            } )

        toReturnEdgeList.filter( t => t._2.size() > 0 )
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