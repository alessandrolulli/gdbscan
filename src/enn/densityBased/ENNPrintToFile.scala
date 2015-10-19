package enn.densityBased

import org.apache.spark.api.java.JavaPairRDD
import java.util.HashSet
import knn.graph.Node
import util.CCPropertiesImmutable
import org.apache.spark.SparkContext
import scala.collection.JavaConversions._
import knn.graph.INode
import org.apache.spark.rdd.RDD


/**
 * @author alemare
 */
class ENNPrintToFile(config : ENNConfig) extends Serializable
{
    var _time = 0L
    var _lastPrintedOutput = ""
    
    def printENN[TID, T, TN <: INode[TID, T]](rdd : RDD[(TN, HashSet[TID])], iteration : Int) = 
    {
        if(config.printStep > 0 && iteration % config.printStep == 0)
        {
            val graphScala = rdd.map(t => (t._1.getId, t._2.toList.map(u => (u, 0))))
            val toPrint = graphScala.map(t => t._1+"\t"+t._2.map(u => u._1+" "+u._2).mkString(" "))
//            toPrint.coalesce(1, true).saveAsTextFile(config.property.outputFile+"_iter"+iteration)
            toPrint.saveAsTextFile(config.outputFile+"_iter"+iteration)
            _lastPrintedOutput=config.outputFile+"_iter"+iteration
        }
    }
    
    def printTime(iteration : Int, time : Long, totalNode : Long, computingNodes : Long, stoppedNodes : Int, messageNumber : Long)
    {
        _time = _time + time
        val timeToPrint = _time
        
        config.util.io.printStatENN(
                                iteration,
                                timeToPrint, 
                                config.k,
                                config.kMax,
                                config.epsilon,
                                config.randomRestart,
                                config.printStep,
                                totalNode,
                                computingNodes,
                                stoppedNodes,
                                config.performance,
                                messageNumber
                                )
    }
}