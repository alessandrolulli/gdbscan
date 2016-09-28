package enn.densityBased

import java.util.HashSet

import scala.collection.JavaConversions.asScalaSet
import scala.collection.JavaConversions.collectionAsScalaIterable

import org.apache.spark.rdd.RDD

import knn.graph.INode
import knn.graph.NeighborList


/**
 * @author alemare
 */
class ENNPrintToFile(config : ENNConfig) extends Serializable
{
    var _time = 0L

    def printENN[TID, T, TN <: INode[TID, T]](rdd : RDD[(TN, HashSet[TID])], iteration : Int, force : Boolean = false) =
    {
        if(force || (config.printStep > 0 && iteration % config.printStep == 0))
        {
            val graphScala = rdd.map(t => (t._1.getId, t._2.toList.map(u => (u, 0))))
            val toPrint = graphScala.map(t => t._1+"\t"+t._2.map(u => u._1+" "+u._2).mkString(" "))
//            toPrint.coalesce(1, true).saveAsTextFile(config.property.outputFile+"_iter"+iteration)
            if(force)
            {
                toPrint.saveAsTextFile(config.property.outputFile)
            } else
            {
            	toPrint.saveAsTextFile(config.property.outputFile+"_iter"+iteration)
            }
        }
    }

    def printKNN[TID, T, TN <: INode[TID, T]](rdd : RDD[(TN, NeighborList[TID, T, TN])], iteration : Int) =
    {
        if(config.printStep > 0 && iteration % config.printStep == 0)
        {
            val graphScala = rdd.map(t => (t._1.getId, t._2.toList))
            val toPrint = graphScala.map(t => t._1+"\t"+t._2.map(u => u.node.getId+" "+u.similarity).mkString(" "))
//            toPrint.coalesce(1, true).saveAsTextFile(config.property.outputFile+"_iter"+iteration)
            toPrint.saveAsTextFile(config.property.outputFile+"_iter"+iteration)
        }
    }

    def printKNN[TID, T, TN <: INode[TID, T]](rdd : RDD[(TN, NeighborList[TID, T, TN])]) =
    {
        val graphScala = rdd.map(t => (t._1.getId, t._2.toList))
        val toPrint = graphScala.map(t => t._1+"\t"+t._2.map(u => u.node.getId+" "+u.similarity).mkString(" "))
        toPrint.saveAsTextFile(config.property.outputFile)
    }

    def printTime(iteration : Int, time : Long, totalNode : Long, computingNodes : Double, stoppedNodes : Double, messageNumber : Long)
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
                                messageNumber,
                                config
                                )
    }
}