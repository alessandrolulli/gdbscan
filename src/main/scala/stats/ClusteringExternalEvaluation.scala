package stats

import java.io.FileNotFoundException

import dataset.DatasetLoad
import enn.densityBased.ENNConfig
import knn.graph.INode
import knn.graph.generation.BruteForce
import knn.graph.impl.NodeGeneric
import knn.metric.IMetric
import knn.metric.impl._
import knn.util.{PointND, PointNDBoolean, PointNDSparse}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.reflect.ClassTag
import scala.util.Random

object ClusteringExternalEvaluation {

  def recall[TID: ClassTag](
                                                                          groundthruth : RDD[(TID, TID)],
                                                                          toCheck : RDD[(TID, TID)]) =
  {
    val rddChecked = toCheck.cache
    val rddCheckedSampled = rddChecked.map(t => (t._2, t._1)).groupByKey/*.filter(t => t._2.size > 1000).*/.flatMap(t => {

      val random = new Random
      val array = t._2.toArray

      if(array.size < 10000)
      {
        array.map(u => (u, t._1))
      } else {

        val idSetSample = (1 until Math.min(10000, array.size)).map {
          ttt => array(random.nextInt(array.size))
        }.toSet

        idSetSample.map(u => (u, t._1))
      }
    })

    val rddGround = groundthruth.cache
    val rddGroundSampled = rddGround.map(t => (t._2, t._1)).groupByKey/*.filter(t => t._2.size > 1000).*/.flatMap(t => {

      val random = new Random
      val array = t._2.toArray

      if(array.size < 10000)
        {
          array.map(u => (u, t._1))
        } else {

        val idSetSample = (1 until Math.min(10000, array.size)).map {
          ttt => array(random.nextInt(array.size))
        }.toSet

        idSetSample.map(u => (u, t._1))
      }
    })

    val joined = rddGroundSampled.leftOuterJoin(rddChecked).map(t => (t._2._1, t._2._2)).groupByKey.map(t => {

      val array = t._2.toArray

      var TP = 0
      var FN = 0

      for (i <- 0 to array.size - 2)
      {
        if(array(i).isDefined)
        {
          for (j <- i + 1 to array.size - 1)
          {
            if (array(j).isDefined && array(i).get == array(j).get) {TP = TP + 1}
            else {FN = FN + 1}
          }
        } else
        {
          FN = FN + array.size - 1
        }
      }

      (TP, FN)
    }).reduce((a,b) => (a._1 + b._1, a._2 + b._2))

    val joinedFP = rddCheckedSampled.leftOuterJoin(rddGround).map(t => (t._2._1, t._2._2)).groupByKey.map(t => {

      val array = t._2.toArray

      var FP = 0

      for (i <- 0 to array.size - 2)
      {
        if(array(i).isDefined)
        {
          for (j <- i + 1 to array.size - 1)
          {
            if (array(j).isDefined && array(i).get != array(j).get) {FP = FP + 1}

          }
        }
      }

      FP
    }).reduce(_+_)

    val recall = joined._1.toDouble / (joined._1 + joined._2)
    val precision = joined._1.toDouble / (joined._1 + joinedFP)

    // recall , precision, f-measure
    (recall, precision, (2*precision*recall) / (precision + recall))
  }

  def main(args_ : Array[String], sc: SparkContext): Unit = {
    // super ugly code! make it better!
    val config = new ENNConfig(args_, "CLUSTERING_EXTERNAL_EVALUATION")
//    val sc = config.util.getSparkContext();

    val groundSplit = config.propertyLoad.get("ennType", "String") match {
      case "ImageBinary" | "ImageBinaryMAP" => ","
      case "Transaction" | "TransactionMAP" => ";"
      case _ => "\t"
    }
    val file = sc.textFile(config.property.dataset, config.property.sparkPartition).cache
    val cluster = DatasetLoad.loadCluster(sc.textFile(config.property.outputFile + "_CLUSTERING", config.property.sparkPartition)).cache
    val groundthruth = DatasetLoad.loadCluster(sc.textFile(config.groundtruth), groundSplit)

    val dataSize = file.count
    val clusteredDataSize = cluster.count
    val clusterInfo = cluster.map(t => (t._2, 1)).reduceByKey(_ + _).cache()
    val clusterNumber = clusterInfo.count
    val clusterMaxSize = clusterInfo.map(t => t._2).max

    val groundClusterNumber = groundthruth.map(t => (t._2, 1)).reduceByKey(_ + _).count

    def printOutput(recall : (Double, Double, Double), deltaK : Double) = {
      config.util.io.printData("externalEvaluation.txt",
        config.property.dataset,
        config.epsilon.toString,
        config.property.coreThreshold.toString,
        dataSize.toString,
        clusteredDataSize.toString,
        clusterNumber.toString,
        clusterMaxSize.toString,
        recall._1.toString,
        recall._2.toString,
        recall._3.toString,
        deltaK.toString)
    }

    val recallValue = recall[Long](groundthruth, cluster)
    val deltaK = groundClusterNumber.toDouble - clusterNumber

    printOutput(recallValue, deltaK)

    sc.stop
  }
}