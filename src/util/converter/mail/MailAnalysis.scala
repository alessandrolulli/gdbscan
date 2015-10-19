package util.converter.mail

import util.CCProperties
import util.CCUtil
import util.CCProperties
import java.io.FileNotFoundException
import util.CCPropertiesImmutable
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import com.google.common.base.Joiner
import java.io.FileWriter

object MailAnalysis 
{
    // return (id, subject) OR (id, seed)
    def loadDataset(data : RDD[String], property : CCPropertiesImmutable) : RDD[(String, String)] = 
	{
		val toReturn = data.map(line =>
		{
			val splitted = line.split(property.separator)
			if (splitted.size > 1) 
			{
				(splitted(0), splitted(1))
			} else if(splitted.size == 1)
			{
			    (splitted(0), "")
			}
			else 
			{
				throw new FileNotFoundException("impossible to parse: "+line)
			}
		})
			
		toReturn
	}
    
    def main( args : Array[String] ) : Unit = 
    {
        val propertyLoad = (new CCProperties("CONVERTER_MAIL", args(0))).load()
		val property = propertyLoad.getImmutable
		val feature = propertyLoad.get("feature", "")
		
		val util = new CCUtil(property);
		val sc = util.getSparkContext();

		val dataset = loadDataset(sc.textFile(property.dataset, property.sparkPartition), property).cache // id, feature
		val size = dataset.count
		val datasetFeature = dataset.map(t => (t._2, 1)).reduceByKey{ case(a,b)=>a+b}
		
		val uniqueSize = datasetFeature.count
		
		val large = datasetFeature.takeOrdered(5)(new OrderingMail).map(t => t._1+","+t._2)
		
		
//		val largest = datasetFeature.reduce{ case(a,b) => {if (a._2 > b._2) a else b}}
//		val largestNotEmpty = datasetFeature.filter(t => t._1 != "").reduce{ case(a,b) => {if (a._2 > b._2) a else b}}
		
		val printFile = new FileWriter( "data.txt", true )
		val joiner = Joiner.on(",")
		val token : Array[Object] = Array(	feature, 
											size.toString,
											uniqueSize.toString,
											large.mkString(","))
		
		printFile.write(joiner.join(token)+ "\n" )
        printFile.close
        
//        val printFileDistribution = new FileWriter( "dataDistribution.txt", true )
//		
//		val distribution = datasetFeature.map(t=>(t._2,1)).reduceByKey{case(a,b)=>a+b}.map(t=>feature+","+t._1+","+t._2+"\n").reduce{case(a,b)=>a+b}
//		
//		printFileDistribution.write(distribution+ "\n" )
//		
//        printFileDistribution.close
	}
}