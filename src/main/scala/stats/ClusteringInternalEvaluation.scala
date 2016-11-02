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

  //  def loadMobile(data: RDD[String], property: CCPropertiesImmutable): RDD[(String, PointND)] =
  //    {
  //      val toReturnEdgeList: RDD[(String, PointND)] = data.map(line =>
  //        {
  //          val splitted = line.split(property.separator)
  //          if (!splitted(0).trim.isEmpty) {
  //            //                    try {
  //            (splitted(1), new PointND(splitted.slice(2, splitted.size).map(x => x.toDouble)))
  //            //                    } catch {
  //            //                        case e : Exception => ( "EMPTY", PointND.NOT_VALID )
  //
  //            //                    }
  //          } else {
  //            //                    ( "EMPTY", PointND.NOT_VALID )
  //            throw new FileNotFoundException("impossible to parse: " + line)
  //          }
  //        })
  //
  //      toReturnEdgeList
  //      //        toReturnEdgeList.filter( t => t._2.size() > 0 )
  //    }

  //  def loadExemplar(data: RDD[String], property: CCPropertiesImmutable): RDD[(String, PointND)] =
  //    {
  //      val toReturnEdgeList: RDD[(String, PointND)] = data.map(line =>
  //        {
  //          val splitted = line.split(property.separator)
  //          if (!splitted(0).trim.isEmpty) {
  //            //                    try {
  //            (splitted(0), new PointND(splitted.slice(3, splitted.size).map(x => x.toDouble)))
  //            //                    } catch {
  //            //                        case e : Exception => ( "EMPTY", PointND.NOT_VALID )
  //
  //            //                    }
  //          } else {
  //            //                    ( "EMPTY", PointND.NOT_VALID )
  //            throw new FileNotFoundException("impossible to parse: " + line)
  //          }
  //        })
  //
  //      toReturnEdgeList
  //      //        toReturnEdgeList.filter( t => t._2.size() > 0 )
  //    }

  def loadCluster(data: RDD[String]): RDD[(Long, Long)] =
    {
      val toReturn = data.map(line =>
        {
          val splitted = line.split("\t")
          if (splitted.size > 1) {
            (splitted(0).toLong, splitted(1).toLong)
          } else {
            throw new FileNotFoundException("impossible to parse: " + line)
          }
        })

      toReturn
    }

  //  def separationCalculation(rddExemplar: RDD[(String, PointND)], rddCC: RDD[(String, String)], rddSubject: RDD[(String, PointND)], property: CCPropertiesImmutable, sc: SparkContext, alpha: Double): (scala.collection.mutable.Map[String, Double], Double, Double) =
  //    {
  //      rddExemplar.cache.first
  //      val rddJoinedAll = rddCC.join(rddSubject).cache // id , (seed, subject)
  //      rddJoinedAll.first
  //
  //      //		for( a <- 1 to 10)
  //      //		{
  //      val rddPrepareAll = rddJoinedAll.map(t => (t._2._1, t._2._2)).groupByKey.map(t => new Node[PointND](t._1, Random.shuffle(t._2.toList).head)).cache
  //      val rddPrepare = rddExemplar.map(t => new Node[PointND](t._1, t._2)).cache
  //
  //      val toCheck = rddPrepare.collect.toList
  //      val toCheckAll = rddPrepareAll.collect.toList
  //
  //      val brute = new Brute[String, PointND, Node[PointND]]();
  //      brute.setK(5);
  //      //        brute.setSimilarity(new EuclidianDistanceND[String, PointND, Node[PointND]]);
  //      brute.setSimilarity(new MobileSimilarityCompositeND[String, PointND, Node[PointND]](alpha));
  //
  //      val exact_graph = brute.computeGraph(toCheck);
  //      val exact_graphAll = brute.computeGraph(toCheckAll);
  //
  //      val minimalInterClusterDistance = exact_graph.map(t => (1.0 - t._2.getMaxSimilarity().similarity)).min
  //      val silhoutteB = exact_graph.map(t => (t._1.id, (1.0 - t._2.getMaxSimilarity().similarity)))
  //      val separationCalculation = exact_graph.map(t => t._2.getMaxSimilarity().similarity)
  //      val preProcessing = exact_graph.toSeq
  //      val preProcessingAll = exact_graphAll.toSeq
  //
  //      val data = sc.parallelize(preProcessing, 512).flatMap(t => t._2.map(u => (t._1.getId().toLong, u.node.getId().toLong, u.similarity)))
  //        .map(t => ((Math.min(t._1, t._2), Math.max(t._1, t._2)), t._3)).reduceByKey((a, b) => Math.max(a, b))
  //        .collect
  //      val dataAll = sc.parallelize(preProcessingAll, 512).flatMap(t => t._2.map(u => (t._1.getId().toLong, u.node.getId().toLong, u.similarity)))
  //        .map(t => ((Math.min(t._1, t._2), Math.max(t._1, t._2)), t._3)).reduceByKey((a, b) => Math.max(a, b))
  //        .collect
  //
  //      val printFile = new FileWriter("separation_exemplar.txt", true)
  //      val joiner = Joiner.on(",")
  //
  //      for (line <- data) {
  //
  //        // description = algorithmName,dataset,dataset2,custom,k,iteration,edgeThreshold,minimalInterClusterDistance,maximalIntraClusterDistance,dunn,separation,compactness,silhoutte,validClusterNumber
  //        val token: Array[Object] = Array(
  //          property.dataset2,
  //          //											a.toString,
  //          line._1._1.toString,
  //          line._1._2.toString,
  //          (1 - line._2).toString)
  //
  //        printFile.write(joiner.join(token) + "\n")
  //        //		}
  //
  //      }
  //      printFile.close
  //
  //      val printFileAll = new FileWriter("separation.txt", true)
  //
  //      for (line <- dataAll) {
  //
  //        // description = algorithmName,dataset,dataset2,custom,k,iteration,edgeThreshold,minimalInterClusterDistance,maximalIntraClusterDistance,dunn,separation,compactness,silhoutte,validClusterNumber
  //        val token: Array[Object] = Array(
  //          property.dataset2,
  //          //											a.toString,
  //          line._1._1.toString,
  //          line._1._2.toString,
  //          (1 - line._2).toString)
  //
  //        printFileAll.write(joiner.join(token) + "\n")
  //        //		}
  //
  //      }
  //      printFileAll.close
  //
  //      val separation = separationCalculation.sum / separationCalculation.size
  //
  //      (silhoutteB, separation, minimalInterClusterDistance)
  //    }

  def separation[TID: ClassTag, T: ClassTag, TN <: INode[TID, T]: ClassTag](
    similarity: IMetric[TID, T, TN],
    rddCC: RDD[(TID, TID)],
    rddSubject: RDD[(TID, TN)],
    sc: SparkContext): Double =
    {
      val rddJoined = rddCC.join(rddSubject) // id , (seed, subject)
        .map(t => (t._2._1, t._2._2))
        .groupByKey
        .map(t => Random.shuffle(t._2.toList).head)
        .collect.
        toList

      val brute = new BruteForce[TID, T, TN]();
      brute.setK(5);
      brute.setSimilarity(similarity);

      val exactGraph = brute.computeGraph(rddJoined);

      val separationCalculation = exactGraph.map(t => t._2.getMaxSimilarity().similarity)

      val separation = separationCalculation.sum / separationCalculation.size

      separation
    }

  def compactness[TID: ClassTag, T: ClassTag, TN <: INode[TID, T]: ClassTag](
    similarity: IMetric[TID, T, TN],
    rddCC: RDD[(TID, TID)],
    rddSubject: RDD[(TID, TN)],
    sc: SparkContext): Double =
    {
      val asp = rddCC.join(rddSubject) // (id, (clusterId, subject)
      val clusterIdSet = asp.map(t => (t._2._1, 1)).groupByKey.map(t => t._1).collect
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

        (clusterId, exact_graph._2, size)
      }), 64)

      val compactnessSum = kkk.map(t => t._2).reduce(_ + _)
      val compactnessCount = kkk.map(t => t._2).count
      val compactness = compactnessSum / compactnessCount

      compactness
    }

  def computeInternalEvaluation[TID: ClassTag, T: ClassTag, TN <: INode[TID, T]: ClassTag](similarity: IMetric[TID, T, TN],
                                                                                           rddCC: RDD[(TID, TID)],
                                                                                           rddSubject: RDD[(TID, TN)],
                                                                                           sc: SparkContext,
                                                                                           config : ENNConfig) =
    {
      val separationValue = separation(similarity, rddCC, rddSubject, sc)
      val compactnessValue = compactness(similarity, rddCC, rddSubject, sc)
      
      config.util.io.printData("internalEvaluation.txt", 
                                config.property.dataset,
                                config.epsilon.toString,
                                config.property.coreThreshold.toString,
                                separationValue.toString,
                                compactnessValue.toString)
    }

  def main(args_ : Array[String]): Unit =
    {
      // super ugly code! make it better!
      val config = new ENNConfig(args_, "CLUSTERING_INTERNAL_EVALUATION")
      val sc = config.util.getSparkContext();

      val file = sc.textFile(config.property.dataset, config.property.sparkPartition)
      val cluster = loadCluster(sc.textFile(config.property.outputFile + "_CLUSTERING", config.property.sparkPartition))

      config.propertyLoad.get("ennType", "String") match {
        case "BagOfWords" | "BagOfWordsMAP" =>
          {
            val vertexRDD = DatasetLoad.loadBagOfWords(file, config.property, config).map(t => (t._1, new NodeGeneric(t._1, t._2)))
            val metric: IMetric[Long, PointNDSparse, NodeGeneric[Long, PointNDSparse]] =
              new CosineSimilarityNDSparse[Long, NodeGeneric[Long, PointNDSparse]]

            computeInternalEvaluation[Long, PointNDSparse, NodeGeneric[Long, PointNDSparse]](metric, cluster, vertexRDD, sc, config)
          }
        case "Transaction" | "TransactionMAP" =>
          {
            val vertexRDD = DatasetLoad.loadTransactionData(file, config.property).map(t => (t._1, new NodeGeneric(t._1, t._2)))
            val metric: IMetric[Long, java.util.Set[Int], NodeGeneric[Long, java.util.Set[Int]]] =
              new JaccardSimilaritySet[Long, Int, java.util.Set[Int], NodeGeneric[Long, java.util.Set[Int]]]

            computeInternalEvaluation[Long, java.util.Set[Int], NodeGeneric[Long, java.util.Set[Int]]](metric, cluster, vertexRDD, sc, config)
          }
        case "ImageBinary" | "ImageBinaryMAP" =>
          {
            val vertexRDD = DatasetLoad.loadImageBinary(file, config.property, config).map(t => (t._1, new NodeGeneric(t._1, t._2)))
            val metric: IMetric[Long, PointNDBoolean, NodeGeneric[Long, PointNDBoolean]] =
              new SimpsonScore[Long, NodeGeneric[Long, PointNDBoolean]]

            computeInternalEvaluation[Long, PointNDBoolean, NodeGeneric[Long, PointNDBoolean]](metric, cluster, vertexRDD, sc, config)
          }
          case "Household" | "HouseholdMAP" =>
                {
                    val vertexRDD = DatasetLoad.loadHousehold( file , config.property).map(t => (t._1, new NodeGeneric(t._1, t._2)))
                    val metric : IMetric[Long, PointND, NodeGeneric[Long, PointND]] = 
                      new EuclidianDistanceND[Long, PointND, NodeGeneric[Long, PointND]]
                    
                    computeInternalEvaluation[Long, PointND, NodeGeneric[Long, PointND]](metric, cluster, vertexRDD, sc, config)
                }
          case "String" | "StringMAP" =>
                {
                    val vertexRDD = DatasetLoad.loadStringData( file , config.property, config).map(t => (t._1.toLong, new NodeGeneric(t._1.toLong, t._2)))
                    val metric = new JaroWinkler[Long, NodeGeneric[Long, String]]
                    
                    computeInternalEvaluation[Long, String, NodeGeneric[Long, String]](metric, cluster, vertexRDD, sc, config)
                }      
             

      }

      sc.stop

      //      util.io.printClusteringQuality(nnDescentK, nnDescentMaxIteration, minimalInterClusterDistance, maximalIntraClusterDistance, separation, compactness, silhoutte, validClusterNumber)
    }
}