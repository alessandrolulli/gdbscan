package util.converter.mail

import util.CCPropertiesImmutable
import org.apache.spark.api.java.JavaSparkContext
import util.CCUtil
import org.apache.spark.rdd.RDD
import util.CCProperties
import info.debatty.java.stringsimilarity.JaroWinkler
import org.apache.spark.api.java.JavaPairRDD
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import knn.graph.NeighborList
import knn.graph.Neighbor
import knn.graph.Node

object ConverterBrute 
{
	def main( args_ : Array[String] ) : Unit = 
    {
	    val timeBegin = System.currentTimeMillis()
	    
	    val propertyLoad = new CCProperties("BRUTE_CONVERTER", args_(0)).load();
		val property = propertyLoad.getImmutable;
		
		val util = new CCUtil(property);
		val sc = util.getJavaSparkContext();
		
		val file = sc.textFile(property.dataset2, property.sparkPartition) // output brute force
//		val rddSubject = util.loadVertexMail(sc.textFile(property.dataset, property.sparkPartition)) // subject dataset
		
		val fusedData = util.loadEdgeFromFileAdjBrute(file)
		
		println("size: "+fusedData.count)
		
		// (id, subject) => only for vertices having all neighbors with similarity 1
//		val vertexEquals = fusedData.filter(t => t._2.map(z => z._2 == 1).reduce((a,b) => a && b)).map(t => (t._1, t._1)).join(rddSubject).map(t => new Node[String](t._1, t._2._2))
//		
//		val jaroWinkler = new JaroWinkler
//        
//        var nndes = new NNDescent[String]
//        nndes = nndes.setK(15)
//        nndes = nndes.setMaxIterations(3)
//        nndes = nndes.setSimilarity(jaroWinkler);
//        
//        var graph = nndes.initializeAndComputeGraph(vertexEquals.toJavaRDD, new NeighborListFactory, property.sparkPartition);
//        
//        val graphSize = graph.cache().count;
//        val graphScala = JavaPairRDD.toRDD(graph).map(t => (t._1.id, t._2.convertToList().toList.map(z => (z.node.id, z.similarity))))
//        
//        val K20pre = fusedData.leftOuterJoin(graphScala).map(t => {if (t._2._2.isDefined) (t._1, t._2._2.get.toIterable) else (t._1, t._2._1)}).map(t => (t._1, new NeighborList(t._2.map(k => new Neighbor(new Node(k._1, ""), k._2)).toList))).cache
        
        
        //////////////7VALID
//        val K20pre = fusedData.map(t => (t._1, new NeighborList(t._2.map(k => new Neighbor(new Node(k._1, ""), k._2)).toList))).cache
//        
//        val K20 = K20pre.map(t => (t._1, t._2.convertWithSize(20))).cache
//        
//	    val toPrintK20 = K20.map(t => t._1+"\t"+t._2.getNeighbourId())
//	    toPrintK20.coalesce(1, true).saveAsTextFile(property.outputFile+"_K20")
//	    
//	    val K15 = K20.map(t => (t._1, t._2.convertWithSize(15))).cache
//	    
//	    val toPrintK15 = K15.map(t => t._1+"\t"+t._2.getNeighbourId())
//	    toPrintK15.coalesce(1, true).saveAsTextFile(property.outputFile+"_K15")
//	    
//	    val K10 = K15.map(t => (t._1, t._2.convertWithSize(10))).cache
//	    
//	    val toPrintK10 = K10.map(t => t._1+"\t"+t._2.getNeighbourId())
//	    toPrintK10.coalesce(1, true).saveAsTextFile(property.outputFile+"_K10")
//	    
//	    val K5 = K10.map(t => (t._1, t._2.convertWithSize(5))).cache
//	    
//	    val toPrintK5 = K5.map(t => t._1+"\t"+t._2.getNeighbourId())
//	    toPrintK5.coalesce(1, true).saveAsTextFile(property.outputFile+"_K5")
    }
}