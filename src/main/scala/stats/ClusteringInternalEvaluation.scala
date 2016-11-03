package stats

import java.io.FileNotFoundException
import scala.collection.JavaConversions._
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.mutable.MultiMap
import scala.util.Random
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import knn.util.Point2D
import knn.util.PointND
import com.google.common.base.Joiner
import java.io.FileWriter
import org.apache.spark.SparkContext
import dataset.DatasetLoad
import knn.metric.impl.JaroWinkler
import enn.densityBased.ENNConfig
import knn.graph.impl.Node
import knn.graph.INode
import knn.metric.IMetric
import knn.graph.generation.BruteForce
import scala.reflect.ClassTag
import org.apache.spark.SparkContext.doubleRDDToDoubleRDDFunctions
import knn.util.PointNDSparse
import knn.graph.impl.NodeGeneric
import knn.metric.impl.CosineSimilarityNDSparse
import knn.metric.impl.JaccardSimilaritySet
import knn.metric.impl.SimpsonScore
import knn.util.PointNDBoolean
import knn.metric.impl.EuclidianDistanceND

object ClusteringInternalEvaluation {

  def separationLoop[TID: ClassTag, T: ClassTag, TN <: INode[TID, T] : ClassTag](
                                                                              similarity: IMetric[TID, T, TN],
                                                                              rddCC: RDD[(TID, TID)],
                                                                              rddSubject: RDD[(TID, TN)],
                                                                              sc: SparkContext): (Double, Double) = {
    val rddJoined = rddCC.join(rddSubject) // id , (seed, subject)
      .map(t => (t._2._1, t._2._2))
      .groupByKey
      .map(t => (Random.shuffle(t._2.toList).head, t._2.size)).cache
    val size = rddJoined.count

    val sampleSize = Math.min(1.0, 2000.0 / size)
    val forBruteForce = rddJoined.map(t => t._1).sample(false, sampleSize, System.currentTimeMillis()).collect.toList

    val brute = new BruteForce[TID, T, TN]();
    brute.setK(5);
    brute.setSimilarity(similarity);

    val exactGraph = brute.computeGraph(forBruteForce);

    val separationCalculation = exactGraph.map(t => t._2.getMaxSimilarity().similarity)
    val separation = separationCalculation.sum / separationCalculation.size

    val weighted = rddJoined.map(t => (t._1.getId, t._2))
      .join(sc.parallelize(exactGraph.map(t => (t._1.getId, t._2.getMaxSimilarity().similarity)).toSeq))
      .map(t => (t._2._1, t._2._2))
      .reduce((a, b) => (a._1 + b._1, (a._1 * a._2 + b._1 * b._2)/(a._1 + b._1)))

    (separation, weighted._2)
  }

  def separation[TID: ClassTag, T: ClassTag, TN <: INode[TID, T] : ClassTag](
                                                                              similarity: IMetric[TID, T, TN],
                                                                              rddCC: RDD[(TID, TID)],
                                                                              rddSubject: RDD[(TID, TN)],
                                                                              sc: SparkContext): (Double, Double) = {
    val repetition = 10
    val result = for ( i <- 1 to repetition)  yield separationLoop(similarity, rddCC, rddSubject, sc)

    val toReturn = result.reduce((a,b) => (a._1 + b._1, a._2 + b._2))

    (toReturn._1 / repetition, toReturn._2 / repetition)
  }

  def compactness[TID: ClassTag, T: ClassTag, TN <: INode[TID, T] : ClassTag](
                                                                               similarity: IMetric[TID, T, TN],
                                                                               rddCC: RDD[(TID, TID)],
                                                                               rddSubject: RDD[(TID, TN)],
                                                                               sc: SparkContext): (Double, Double) = {
    val asp = rddCC.join(rddSubject) // (id, (clusterId, subject)
    val clusterIdSet = asp.map(t => (t._2._1, 1)).groupByKey.filter(t => t._2.size > 1).map(t => t._1).collect
    val validClusterNumber = clusterIdSet.size

    val useIt = asp.map(z => (z._2._1, (z._2._1, z._1, z._2._2))).cache
    useIt.first

    val random = new Random
    var clusterIdSetSample = clusterIdSet
    if (clusterIdSetSample.size > 300) {
      clusterIdSetSample = (1 until 300).map {
        ttt => clusterIdSet(random.nextInt(clusterIdSet.size))
      }.toArray
    }

    val asp3 = clusterIdSetSample.map(t => useIt.filter { case (key, value) => key == t }.values)
    val asp3size = asp3.size

    val kkk = sc.parallelize(asp3.map(z => {

      val groupSubject = z.map(u => u._3)
      val clusterId = z.map(u => u._1).first

      var sampleSize = 0.01
      val size = groupSubject.count

      if (size < 2000) sampleSize = 1
      else sampleSize = 2000.0 / size

      val toCheck = groupSubject.sample(false, sampleSize, System.currentTimeMillis()).collect
      val sizeCheck = toCheck.size

      val brute = new BruteForce[TID, T, TN]();
      brute.setK(5);
      brute.setSimilarity(similarity);
      val exact_graph = brute.computeGraphAndAvgSimilarity(toCheck.toList.asJava);

      (clusterId, exact_graph._2, sizeCheck)
    }), 64)

    val compactnessSum = kkk.map(t => t._2).reduce(_ + _)
    val compactnessCount = kkk.count
    val compactness = compactnessSum / compactnessCount

    val weighted = kkk.map(t => (t._3, t._2)).reduce((a, b) => (a._1 + b._1, (a._1 * a._2 + b._1 * b._2)/(a._1 + b._1)))

    (compactness, weighted._2)
  }

  def computeInternalEvaluation[TID: ClassTag, T: ClassTag, TN <: INode[TID, T] : ClassTag](similarity: IMetric[TID, T, TN],
                                                                                            rddCC: RDD[(TID, TID)],
                                                                                            rddSubject: RDD[(TID, TN)],
                                                                                            sc: SparkContext,
                                                                                            config: ENNConfig) = {
    val separationValue = separation(similarity, rddCC, rddSubject, sc)
    val compactnessValue = compactness(similarity, rddCC, rddSubject, sc)

    (separationValue, compactnessValue)
  }

  def main(args_ : Array[String]): Unit = {
    // super ugly code! make it better!
    val config = new ENNConfig(args_, "CLUSTERING_INTERNAL_EVALUATION")
    val sc = config.util.getSparkContext();

    val file = sc.textFile(config.property.dataset, config.property.sparkPartition).cache
    val cluster = DatasetLoad.loadCluster(sc.textFile(config.property.outputFile + "_CLUSTERING", config.property.sparkPartition)).cache

    val dataSize = file.count
    val clusteredDataSize = cluster.count
    val clusterInfo = cluster.map(t => (t._2, 1)).reduceByKey(_ + _).cache()
    val clusterNumber = clusterInfo.count
    val clusterMaxSize = clusterInfo.map(t => t._2).max

    def printOutput(separationValue : (Double, Double), compactnessValue : (Double, Double)) = {
      config.util.io.printData("internalEvaluation.txt",
        config.property.dataset,
        config.epsilon.toString,
        config.property.coreThreshold.toString,
        dataSize.toString,
        clusteredDataSize.toString,
        clusterNumber.toString,
        clusterMaxSize.toString,
        separationValue._1.toString,
        separationValue._2.toString,
        compactnessValue._1.toString,
        compactnessValue._2.toString)
    }

    config.propertyLoad.get("ennType", "String") match {
      case "BagOfWords" | "BagOfWordsMAP" => {
        val vertexRDD = DatasetLoad.loadBagOfWords(file, config.property, config).map(t => (t._1, new NodeGeneric(t._1, t._2)))
        val metric: IMetric[Long, PointNDSparse, NodeGeneric[Long, PointNDSparse]] =
          new CosineSimilarityNDSparse[Long, NodeGeneric[Long, PointNDSparse]]

        val (separationValue, compactnessValue) = computeInternalEvaluation[Long, PointNDSparse, NodeGeneric[Long, PointNDSparse]](metric, cluster, vertexRDD, sc, config)
        printOutput(separationValue, compactnessValue)
      }
      case "Transaction" | "TransactionMAP" => {
        val vertexRDD = DatasetLoad.loadTransactionData(file, config.property).map(t => (t._1, new NodeGeneric(t._1, t._2)))
        val metric: IMetric[Long, java.util.Set[Int], NodeGeneric[Long, java.util.Set[Int]]] =
          new JaccardSimilaritySet[Long, Int, java.util.Set[Int], NodeGeneric[Long, java.util.Set[Int]]]

        val (separationValue, compactnessValue) = computeInternalEvaluation[Long, java.util.Set[Int], NodeGeneric[Long, java.util.Set[Int]]](metric, cluster, vertexRDD, sc, config)
        printOutput(separationValue, compactnessValue)
      }
      case "ImageBinary" | "ImageBinaryMAP" => {
        val vertexRDD = DatasetLoad.loadImageBinary(file, config.property, config).map(t => (t._1, new NodeGeneric(t._1, t._2)))
        val metric: IMetric[Long, PointNDBoolean, NodeGeneric[Long, PointNDBoolean]] =
          new SimpsonScore[Long, NodeGeneric[Long, PointNDBoolean]]

        val (separationValue, compactnessValue) = computeInternalEvaluation[Long, PointNDBoolean, NodeGeneric[Long, PointNDBoolean]](metric, cluster, vertexRDD, sc, config)
        printOutput(separationValue, compactnessValue)
      }
      case "Household" | "HouseholdMAP" => {
        val vertexRDD = DatasetLoad.loadHousehold(file, config.property).map(t => (t._1, new NodeGeneric(t._1, t._2)))
        val metric: IMetric[Long, PointND, NodeGeneric[Long, PointND]] =
          new EuclidianDistanceND[Long, PointND, NodeGeneric[Long, PointND]]

        val (separationValue, compactnessValue) = computeInternalEvaluation[Long, PointND, NodeGeneric[Long, PointND]](metric, cluster, vertexRDD, sc, config)
        printOutput(separationValue, compactnessValue)
      }
      case "String" | "StringMAP" => {
        val vertexRDD = DatasetLoad.loadStringData(file, config.property, config).map(t => (t._1.toLong, new NodeGeneric(t._1.toLong, t._2)))
        val metric = new JaroWinkler[Long, NodeGeneric[Long, String]]

        val (separationValue, compactnessValue) = computeInternalEvaluation[Long, String, NodeGeneric[Long, String]](metric, cluster, vertexRDD, sc, config)
        printOutput(separationValue, compactnessValue)
      }
    }

    sc.stop
  }
}