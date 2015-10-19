//package util.converter.mail
//
//import java.io.FileNotFoundException
//import java.io.FileWriter
//import java.util.Random
//
//import scala.collection.JavaConversions.seqAsJavaList
//
//import org.apache.spark.SparkContext.rddToPairRDDFunctions
//import org.apache.spark.rdd.RDD
//
//import com.google.common.base.Joiner
//
//import info.debatty.java.graphs.build.Brute
//import info.debatty.java.stringsimilarity.JaroWinkler
//import knn.graph.Node
//import util.CCProperties
//import util.CCPropertiesImmutable
//import util.CCUtil
//
//object MailGroupAnalysis 
//{
//    // return (id, subject) OR (id, seed)
//    def loadDataset(data : RDD[String], property : CCPropertiesImmutable) : RDD[(String, String)] = 
//	{
//		val toReturn = data.map(line =>
//		{
//			val splitted = line.split(property.separator)
//			if (splitted.size > 1) 
//			{
//				(splitted(0), splitted(1))
//			} else if(splitted.size == 1)
//			{
//			    (splitted(0), "")
//			}
//			else 
//			{
//				throw new FileNotFoundException("impossible to parse: "+line)
//			}
//		})
//			
//		toReturn
//	}
//    
//    def main( args : Array[String] ) : Unit = 
//    {
//        val propertyLoad = (new CCProperties("MAILGROUPANALYSIS", args(0))).load()
//		val property = propertyLoad.getImmutable
//		val feature = propertyLoad.get("feature", "")
//		
//		val util = new CCUtil(property);
//		val sc = util.getSparkContext();
//
//		val dataset = loadDataset(sc.textFile(property.dataset2, property.sparkPartition), property).cache // id, feature
////		val rddSubject = util.loadVertexMail(sc.textFile(property.dataset, property.sparkPartition))
//		
//		val joinedRdd = dataset.join(util.loadVertexMail(sc.textFile(property.dataset, property.sparkPartition))) // id(feature, subject)
//		
//		val jaroWinkler = new JaroWinkler
//		val groupedRdd = joinedRdd.map(t => (t._2._1, (t._1, t._2._2))).cache
//		
//		val asp2 = joinedRdd.map(t => (t._2._1, 1)).groupByKey.filter(t => t._2.size > 1000).map(t => t._1).collect
//		
//		val random = new Random
//		var featureIdSetSample = asp2
//		if(featureIdSetSample.size > 200)
//		{
//			featureIdSetSample = (1 until 150).map
//			{
//			    ttt => asp2(random.nextInt(asp2.size))
//			}.toArray
//		}
//		
//		val asp = sc.parallelize(featureIdSetSample.map(t => groupedRdd.filter { case (key, value) => key == t }.values).map(z =>
//	    {
//	    	val groupSubject = z.map(u => new Node[String](u._1, u._2))
//	    	
//	    	var sampleSize = 0.01
//	    	val size = groupSubject.count
//	    	if(size < 2000) sampleSize = 1
//	    	else if(size * 0.01 > 2000) sampleSize = 0.003
//	    	else if(size * 0.01 < 100) sampleSize = 0.1
//	    	
//	    	val toCheck = groupSubject.sample(true, sampleSize, 12345).collect.toList
//		
//			val brute = new Brute[String]();
//	        brute.setK(5);
//	        brute.setSimilarity(new JaroWinkler);
//	        val exact_graph = brute.computeGraphAndAvgSimilarity(toCheck);
//	    	
//	    	
////	    	var nndes = new NNDescent[String]
////	    	nndes = nndes.setK(5)
////	    	nndes = nndes.setMaxIterations(3)
////	    	nndes = nndes.setSimilarity(jaroWinkler);
////	    	
////	    	var graph = nndes.initializeAndComputeGraph(groupSubject.toJavaRDD, new NeighborListFactory(NeighborList.DESC), 24);
////	    	val graphScala = JavaPairRDD.toRDD(graph)
//	    	
//	    	exact_graph._2
//	    }).toSeq)
//	    
//		
//	    val mean = asp.reduce((a,b)=>a+b) / asp.count
//	    
//	    val printFile = new FileWriter( "stats.txt", true )
//		val joiner = Joiner.on(",")
//		val token : Array[Object] = Array(	property.appName,
//		        							feature, 
//		        							mean.toString
//											)
//		
//		printFile.write(joiner.join(token)+ "\n" )
//        printFile.close
//	    
//	    val result = asp.map(t => ((t * 10).toInt, 1)).reduceByKey((a,b) => a+b)
//		
//	    val printFileDistribution = new FileWriter( "dataGroup.txt", true )
//		
//		val distribution = result.map(t=>feature+","+t._1+","+t._2+"\n").reduce{case(a,b)=>a+b}
//		
//		printFileDistribution.write(distribution+ "\n" )
//		
//        printFileDistribution.close
//	}
//}