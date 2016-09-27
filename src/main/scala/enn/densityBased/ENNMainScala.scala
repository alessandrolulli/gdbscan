package enn.densityBased

import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaRDD.toRDD
import util.CCProperties
import util.CCUtil
import info.debatty.java.stringsimilarity.JaroWinkler
import knn.graph.NeighborListFactory
import knn.graph.Node
import org.apache.spark.rdd.RDD
import knn.graph.IMetric
import knn.util.Point2D

object ENNMainScala 
{
	def main( args_ : Array[String] ) : Unit = 
    {
	    val timeBegin = System.currentTimeMillis
	    
	    val ennLoader = new ENNLoader(args_)
        
        val result = ennLoader.loadAndStart
        
        val timeEnd = System.currentTimeMillis
        
        ennLoader.config.util.io.printStatENN(
                				ennLoader.config.endIterationValue,
                				timeEnd-timeBegin, 
                				ennLoader.config.k,
                				ennLoader.config.kMax,
                				ennLoader.config.epsilon,
                                ennLoader.config.randomRestart,
                                ennLoader.config.printStep,
                                0,
                                0,
                                0,
                                ennLoader.config.performance,
                                0
                				)
                				
        
//        val ennGraphOutput = ennLoader.printer._lastPrintedOutput
//        crackerDensityN.CrackerAllComparable.mainGO(ennGraphOutput, args_)
                                
    }
}