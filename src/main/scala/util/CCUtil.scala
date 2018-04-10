package util

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.io.Source
import org.apache.spark.api.java.JavaSparkContext
import com.google.common.base.Joiner
import java.io.FileWriter
import crackerDensity.CrackerTreeMessageIdentification
import crackerDensity.CrackerAlgorithm

class CCUtil(property: CCPropertiesImmutable) extends Serializable {
  val io = new CCUtilIO(property)
  var vertexNumber = 0L

  def getSparkContext(): SparkContext = {
    val conf = new SparkConf()
      .setMaster(property.sparkMaster)
      .setAppName(property.appName)
      .set("spark.executor.memory", property.sparkExecutorMemory)
      .set("spark.storage.blockManagerSlaveTimeoutMs", property.sparkBlockManagerSlaveTimeoutMs)
      .set("spark.shuffle.manager", property.sparkShuffleManager)
      .set("spark.shuffle.consolidateFiles", property.sparkShuffleConsolidateFiles)
      .set("spark.io.compression.codec", property.sparkCompressionCodec)
      .set("spark.akka.frameSize", property.sparkAkkaFrameSize)
      .set("spark.driver.maxResultSize", property.sparkDriverMaxResultSize)
      .set("spark.core.connection.ack.wait.timeout", 600.toString)
    //				.set("spark.task.cpus", "8")

    if (property.jarPath.endsWith(".jar"))
      conf.setJars(Array(property.jarPath))

    if (property.sparkCoresMax > 0) {
      conf.set("spark.cores.max", property.sparkCoresMax.toString)
      val executorCore = property.sparkCoresMax / property.sparkExecutorInstances
      conf.set("spark.executor.cores", executorCore.toString)
    }
    if (property.sparkExecutorInstances > 0)
      conf.set("spark.executor.instances", property.sparkExecutorInstances.toString)

    val spark = new SparkContext(conf)

    //			spark.setCheckpointDir("hdfs:///user/lulli/tmpCheck")

    spark
  }

  def getJavaSparkContext(): JavaSparkContext = {
    val conf = new SparkConf()
      .setMaster(property.sparkMaster)
      .setAppName(property.appName)
      .set("spark.executor.memory", property.sparkExecutorMemory)
      .set("spark.storage.blockManagerSlaveTimeoutMs", property.sparkBlockManagerSlaveTimeoutMs)
      .set("spark.shuffle.manager", property.sparkShuffleManager)
      .set("spark.shuffle.consolidateFiles", property.sparkShuffleConsolidateFiles)
      .set("spark.io.compression.codec", property.sparkCompressionCodec)
      .set("spark.akka.frameSize", property.sparkAkkaFrameSize)
      .set("spark.driver.maxResultSize", property.sparkDriverMaxResultSize)
      //				.set("spark.task.cpus", "8")
      .setJars(Array(property.jarPath))

    if (property.sparkCoresMax > 0)
      conf.set("spark.cores.max", property.sparkCoresMax.toString)

    if (property.sparkExecutorInstances > 0)
      conf.set("spark.executor.instances", property.sparkExecutorInstances.toString)

    val spark = new SparkContext(conf)

    //			spark.setCheckpointDir(".")

    spark
  }

  // return edgelist and edge associated to each vertex
  def loadEdgeFromFile(data: RDD[String]): (RDD[(Long, Long)], RDD[(Long, Iterable[Long])]) = {
    val toReturnEdgeList = data.flatMap(line => {
      val splitted = line.split(property.separator)
      if (splitted.size >= 1) {
        try {
          if (property.vertexIdMultiplier != -1) {
            Array((splitted(0).toLong * property.vertexIdMultiplier, splitted(1).toLong * property.vertexIdMultiplier), (splitted(1).toLong * property.vertexIdMultiplier, splitted(0).toLong * property.vertexIdMultiplier))
          } else {
            Array((splitted(0).toLong, splitted(1).toLong), (splitted(1).toLong, splitted(0).toLong))
          }
        } catch {
          case e: Exception => Array[(Long, Long)]()
        }
      } else {
        Array[(Long, Long)]()
      }
    })

    //			val toReturnVertex = toReturnEdgeList.distinct.groupByKey
    val toReturnVertex = toReturnEdgeList.groupByKey

    if (property.printMessageStat && property.allStat) {
      val edgeNumber = toReturnEdgeList.count / 2
      vertexNumber = toReturnVertex.count

      val degreeMax = toReturnVertex.map(t => t._2.size).max
      val degreeAvg = toReturnVertex.map(t => t._2.size).mean

      io.printStat(edgeNumber, "edgeNumber")
      io.printStat(vertexNumber, "vertexNumber")
      io.printStat(degreeMax, "degreeMax")
      io.printStat(degreeAvg, "degreeAvg")
    }

    (toReturnEdgeList, toReturnVertex)
  }

  def loadEdgeFromFileComparable(data: RDD[String]): (RDD[(String, String)], RDD[(String, Iterable[String])]) = {
    val toReturnEdgeList = data.flatMap(line => {
      val splitted = line.split(property.separator)
      if (splitted.size >= 1) {
        try {

          Array((splitted(0), splitted(1)), (splitted(1), splitted(0)))
        } catch {
          case e: Exception => Array[(String, String)]()
        }
      } else {
        Array[(String, String)]()
      }
    })

    //			val toReturnVertex = toReturnEdgeList.distinct.groupByKey
    val toReturnVertex = toReturnEdgeList.groupByKey

    if (property.printMessageStat && property.allStat) {
      val edgeNumber = toReturnEdgeList.count / 2
      vertexNumber = toReturnVertex.count

      val degreeMax = toReturnVertex.map(t => t._2.size).max
      val degreeAvg = toReturnVertex.map(t => t._2.size).mean

      io.printStat(edgeNumber, "edgeNumber")
      io.printStat(vertexNumber, "vertexNumber")
      io.printStat(degreeMax, "degreeMax")
      io.printStat(degreeAvg, "degreeAvg")
    }

    (toReturnEdgeList, toReturnVertex)
  }

  def loadEdgeFromFileAdjComparable(data: RDD[String], edgeThreshold: Double, k: Int): (RDD[(String, String)], RDD[(String, Iterable[String])]) = {
    val toReturnEdgeList = data.flatMap(line => {
      val splitted = line.split(property.separator)
      if (splitted.size >= 1 && !splitted(0).trim.isEmpty()) {
        try {
          val splitted2 = splitted(1).split(" ")
          if (splitted2.size > 0) {
            splitted2.map(t => t.trim)
              .filter(t => !t.isEmpty())
              .grouped(2).toArray
              .filter(t => t(1).toDouble > edgeThreshold)
              .flatMap(t => Array((splitted(0), t(0)), (t(0), splitted(0))))
          } else {
            Array[(String, String)]()
          }
        } catch {
          case e: Exception => Array[(String, String)]()
        }
      } else {
        Array[(String, String)]()
      }
    })

    //			val toReturnVertex = toReturnEdgeList.distinct.groupByKey
    val toReturnVertex = toReturnEdgeList.groupByKey

    if (property.printCCDistribution) {
      val distribution = data.flatMap(line => {
        val splitted = line.split(property.separator)
        if (splitted.size >= 1 && !splitted(0).trim.isEmpty()) {
          try {
            val splitted2 = splitted(1).split(" ")
            if (splitted2.size > 0) {
              splitted2.map(t => t.trim)
                .filter(t => !t.isEmpty())
                .grouped(2).toArray
                .filter(t => t(1).toDouble > edgeThreshold)
                .map(t => (splitted(0), t(0)))
            } else {
              Array[(String, String)]()
            }
          } catch {
            case e: Exception => Array[(String, String)]()
          }
        } else {
          Array[(String, String)]()
        }
      })

      val edgesRemovedDistribution = distribution.groupByKey.map(t => (k - t._2.size, 1)).reduceByKey { case (a, b) => a + b }

      val joiner = Joiner.on(",")

      val printFile = new FileWriter("distributionEdgeRemoved.txt", true)

      val token: Array[Object] = Array(property.dataset,
        k.toString,
        edgeThreshold.toString)
      val tokenToString = joiner.join(token)

      // description = dataset,k,edgeThreshold,edgeRemoved,number
      val toPrint = edgesRemovedDistribution.map(t => tokenToString + "," + t._1 + "," + t._2 + "\n").reduce { case (a, b) => a + b }
      printFile.write(toPrint)

      printFile.close
    }

    if (property.printMessageStat && property.allStat) {
      val edgeNumber = toReturnEdgeList.count / 2
      vertexNumber = toReturnVertex.count

      val degreeMax = toReturnVertex.map(t => t._2.size).max
      val degreeAvg = toReturnVertex.map(t => t._2.size).mean

      io.printStat(edgeNumber, "edgeNumber")
      io.printStat(vertexNumber, "vertexNumber")
      io.printStat(degreeMax, "degreeMax")
      io.printStat(degreeAvg, "degreeAvg")
    }

    (toReturnEdgeList, toReturnVertex)
  }

  def loadEdgeFromFileAdjComparableDegree
  (data: RDD[String], crackerAlgorithm: CrackerAlgorithm)
  : RDD[(Long, CrackerTreeMessageIdentification)] = {
    /*
     * in this method i already perform the communication of the self degree to all neighbours
     */

    val toReturnEdgeList = data.flatMap(line => {
      val splitted = line.split("\t")
      if (splitted.size >= 1 && !splitted(0).trim.isEmpty()) {
        try {
          val splitted2 = splitted(1).split(" ")
          if (splitted2.size > 0) {
            splitted2.map(t => t.trim)
              .filter(t => !t.isEmpty())
              .grouped(2).toArray
              .flatMap(t => Array((splitted(0).toLong, t(0).toLong), (t(0).toLong, splitted(0).toLong)))
          } else {
            Array[(Long, Long)]()
          }
        } catch {
          case e: Exception => Array[(Long, Long)]()
        }
      } else {
        Array[(Long, Long)]()
      }
    })

    val toReturnVertex = toReturnEdgeList.groupByKey.map(t => (t._1, t._2.toSet + t._1)) //.map(t => (t._1, t._2.filter(t => !t.isEmpty() && t != " ")))

    val sendingDegree = toReturnVertex.flatMap(t => t._2.map(u => {
      if (t._1 == u) {
        (u, CrackerTreeMessageIdentification.apply(t._2.size, t._1, t._2.size))
      } else {
        (u, CrackerTreeMessageIdentification.apply(-1, t._1, t._2.size))
      }
    }))
      .reduceByKey(crackerAlgorithm.reduceBlue)
    //                .flatMap(t => t._2.neigh.map(u =>
    //                    {
    //                        if(t._1 == u._1)
    //                        {
    //                            (u._1, t._2)
    //                        } else
    //                        {
    //                            (u._1, CrackerTreeMessageIdentification.apply(-1, t._1, t._2.selfDegree))
    //                        }
    //                    })).reduceByKey(crackerAlgorithm.reduceBlue)

    //			if (property.printMessageStat && property.allStat) {
    //				val edgeNumber = toReturnEdgeList.count / 2
    //				vertexNumber = toReturnVertex.count
    //
    //				val degreeMax = toReturnVertex.map(t => t._2.size).max
    //				val degreeAvg = toReturnVertex.map(t => t._2.size).mean
    //
    //				io.printStat(edgeNumber, "edgeNumber")
    //				io.printStat(vertexNumber, "vertexNumber")
    //				io.printStat(degreeMax, "degreeMax")
    //				io.printStat(degreeAvg, "degreeAvg")
    //
    //                io.printStat(sendingDegree.map(t => t._2.neigh.size).sum / 2, "edgeNumberD")
    //                io.printStat(sendingDegree.count, "vertexNumberD")
    //                io.printStat(sendingDegree.map(t => t._2.neigh.size).max, "degreeMaxD")
    //                io.printStat(sendingDegree.map(t => t._2.neigh.size).mean, "degreeAvgD")
    //                io.printStat(sendingDegree.filter(t => t._2.selfDegree >= property.coreThreshold).map(t => 1).sum, "core")
    //			}

    // check
    //            if(sendingDegree.filter(t => t._2.selfDegree < 0).count > 0)
    //            {
    //                throw new NullPointerException("error: selfDegree not valid")
    //            }

    sendingDegree.filter(t => t._2.selfDegree >= property.coreThreshold || (!t._2.neigh.isEmpty && t._2.neigh.map(u => u._2).max >= property.coreThreshold))

    //            sendingDegree.map(t => {
    //                if(t._2.selfDegree >= property.coreThreshold || (!t._2.neigh.isEmpty && t._2.neigh.map(u => u._2).max >= property.coreThreshold))
    //                {
    //                    t
    //                } else
    //                {
    //                    (t._1, new CrackerTreeMessageIdentification(-1, t._2.candidate, t._2.candidateDegree, Map()))
    //                }
    //            })
    //
    //            sendingDegree
  }

  def loadEdgeFromFileAdjMetric(data: RDD[String], edgeThreshold: Double): (RDD[(String, String)], RDD[(String, Iterable[String])]) = {
    val toReturnEdgeList = data.flatMap(line => {
      val splitted = line.split(property.separator)
      if (splitted.size >= 1 && !splitted(0).trim.isEmpty()) {
        try {
          val splitted2 = splitted(1).split(" ")
          if (splitted2.size > 0) {
            splitted2.map(t => t.trim)
              .filter(t => !t.isEmpty())
              .grouped(2).toArray
              .filter(t => t(1).toDouble > edgeThreshold)
              .flatMap(t => Array((splitted(0), t(0)), (t(0), splitted(0))))
          } else {
            Array[(String, String)]()
          }
        } catch {
          case e: Exception => Array[(String, String)]()
        }
      } else {
        Array[(String, String)]()
      }
    })

    //			val toReturnVertex = toReturnEdgeList.distinct.groupByKey
    val toReturnVertex = toReturnEdgeList.groupByKey


    //			if (property.printMessageStat && property.allStat) {
    //				val edgeNumber = toReturnEdgeList.count / 2
    //				vertexNumber = toReturnVertex.count
    //
    //				val degreeMax = toReturnVertex.map(t => t._2.size).max
    //				val degreeAvg = toReturnVertex.map(t => t._2.size).mean
    //
    //				io.printStat(edgeNumber, "edgeNumber")
    //				io.printStat(vertexNumber, "vertexNumber")
    //				io.printStat(degreeMax, "degreeMax")
    //				io.printStat(degreeAvg, "degreeAvg")
    //			}

    (toReturnEdgeList, toReturnVertex)
  }

  def loadEdgeFromFileAdjBrute(data: RDD[String]): RDD[(String, Iterable[(String, Double)])] = {
    val toReturnEdgeList = data.flatMap(line => {
      val splitted = line.split(property.separator)
      if (splitted.size >= 1 && !splitted(0).trim.isEmpty()) {
        try {
          val splitted2 = splitted(1).split(" ")
          if (splitted2.size > 0) {
            splitted2.map(t => t.trim)
              .filter(t => !t.isEmpty())
              .grouped(2).toArray
              //							    			.filter(t => t(1).toDouble > edgeThreshold)
              .flatMap(t => Array((splitted(0), (t(0), t(1).toDouble)), (t(0), (splitted(0), t(1).toDouble))))
          } else {
            Array[(String, (String, Double))]()
          }
        } catch {
          case e: Exception => Array[(String, (String, Double))]()
        }
      } else {
        Array[(String, (String, Double))]()
      }
    })

    toReturnEdgeList.groupByKey
  }

  def loadEdgeFromFileAdjBruteKNN(data: RDD[String]): RDD[(String, Iterable[(String, Double)])] = {
    val toReturnEdgeList = data.flatMap(line => {
      val splitted = line.split(property.separator)
      if (splitted.size >= 1 && !splitted(0).trim.isEmpty()) {
        try {
          val splitted2 = splitted(1).split(" ")
          if (splitted2.size > 0) {
            splitted2.map(t => t.trim)
              .filter(t => !t.isEmpty())
              .grouped(2).toArray
              //							    			.filter(t => t(1).toDouble > edgeThreshold)
              .flatMap(t => Array((splitted(0), (t(0), t(1).toDouble))))
          } else {
            Array[(String, (String, Double))]()
          }
        } catch {
          case e: Exception => Array[(String, (String, Double))]()
        }
      } else {
        Array[(String, (String, Double))]()
      }
    })

    toReturnEdgeList.groupByKey
  }

  def loadVertexMail(data: RDD[String]): RDD[(String, String)] = {
    val toReturnEdgeList: RDD[(String, String)] = data.map(line => {
      val splitted = line.split(property.separator)
      if (splitted.size >= 1 && !splitted(0).trim.isEmpty) {
        try {
          (splitted(0), splitted(1))
        } catch {
          case e: Exception => ("EMPTY", "EMPTY")
        }
      } else {
        ("EMPTY", "EMPTY")
      }
    })

    toReturnEdgeList.filter(t => !t._1.equals("EMPTY"))
  }

  // return edgelist and edge associated to each vertex
  def loadEdgeFromFile(): Array[(Long, Array[Long])] = {
    val toReturnEdgeList = Source.fromFile(property.dataset).getLines.flatMap(line => {
      val splitted = line.split(property.separator)
      if (splitted.size >= 1) {
        try {
          Array((splitted(0).toLong, splitted(1).toLong), (splitted(1).toLong, splitted(0).toLong))
        } catch {
          case e: Exception => Array[(Long, Long)]()
        }
      } else {
        Array[(Long, Long)]()
      }
    }).toArray

    val toReturnVertex = toReturnEdgeList.groupBy(t => t._1).toArray.map { case (group, traversable) => (group, traversable.map(t => t._2)) }


    toReturnVertex
  }

  // load from a file in the format of
  // vertexID, arcID
  def loadVertexEdgeFile(data: RDD[String]): (RDD[(Long, Long)], RDD[(Long, Iterable[Long])]) = {
    def mapToEdgeList(item: (String, Iterable[Long])): Iterable[(Long, Long)] = {
      var outputList: ListBuffer[(Long, Long)] = new ListBuffer

      val it = item._2.iterator

      while (it.hasNext) {
        val next = it.next
        val it2 = item._2.iterator

        while (it2.hasNext) {
          val next2 = it2.next

          if (next != next2) {
            outputList.prepend((next, next2))
          }
        }
      }

      outputList.toIterable
    }

    val toReturnEdgeList = data.flatMap(line => {
      val splitted = line.split(",")
      if (splitted.size >= 1) {
        try {
          Array((splitted(1), splitted(0).toLong))
        } catch {
          case e: Exception => Array[(String, Long)]()
        }
      } else {
        Array[(String, Long)]()
      }
    })

    val edgeList = toReturnEdgeList.groupByKey.flatMap(mapToEdgeList)

    //			io.printEdgelist(edgeList)

    val toReturnVertex = edgeList.groupByKey

    if (property.printMessageStat) {
      val edgeNumber = toReturnEdgeList.count
      val vertexNumber = toReturnVertex.count

      io.printStat(edgeNumber, "edgeNumber")
      io.printStat(vertexNumber, "vertexNumber")
    }

    (edgeList, toReturnVertex)
  }

  def getCCNumber(rdd: RDD[(Long, Int)]) = {
    rdd.count
  }

  def getCCNumber(rdd: Array[(Long, Int)]) = {
    rdd.size
  }

  def getCCNumberNoIsolatedVertices(rdd: RDD[(Long, Int)]) = {
    rdd.filter(t => t._2 != 1).count
  }

  def getCCNumberString(rdd: RDD[(String, Int)]) = {
    rdd.count
  }

  def getCCNumberString(rdd: Array[(String, Int)]) = {
    rdd.size
  }

  def getCCNumberNoIsolatedVerticesString(rdd: RDD[(String, Int)]) = {
    rdd.filter(t => t._2 != 1).count
  }

  def getCCNumberNoIsolatedVertices(rdd: Array[(Long, Int)]) = {
    rdd.filter(t => t._2 != 1).size
  }

  def getCCMaxSize(rdd: RDD[(Long, Int)]) = {
    rdd.map(t => t._2).max
  }

  def getCCMaxSizeString(rdd: RDD[(String, Int)]) = {
    rdd.map(t => t._2).max
  }

  def getCCMaxSizeStringNotNoise(rdd: RDD[(Long, Int)]) = {
    rdd.filter(t => t._1 != "-1").map(t => t._2).max
  }

  def getCCMaxSize(rdd: Array[(Long, Int)]) = {
    rdd.map(t => t._2).max
  }

  def printSimplification(step: Int, activeVertices: Long, activeEdges: Double, degreeMax: Int) = {
    io.printSimplification(step, activeVertices, vertexNumber, activeEdges, degreeMax)
  }

  def printSimplificationDiameter(step: Int, activeVertices: Long, activeEdges: Double, degreeMax: Int) = {
    io.printSimplification(step, activeVertices, vertexNumber, activeEdges, degreeMax)
  }

  def printTimeStep(step: Int, time: Long) = {
    if (!property.printMessageStat)
      io.printTimeStep(step, time)
  }

  def printTimeStepDiameter(step: Int, time: Long) = {
    if (!property.printMessageStat)
      io.printTimeStep(step, time)
  }

  def printMessageStep(step: Int, messageNumber: Long, messageSize: Long) = {
    io.printMessageStep(step, messageNumber, messageSize)
  }

  def testEnded(rdd: RDD[(Long, Int)],
                step: Int,
                timeBegin: Long,
                timeEnd: Long,
                timeSparkLoaded: Long,
                timeDataLoaded: Long,
                reduceInputMessageNumber: Long,
                reduceInputSize: Long,
                bitmaskCustom: String = "000") = {
    io.printTime(timeBegin, timeEnd, "all")
    io.printTime(timeSparkLoaded, timeEnd, "allComputationAndLoadingGraph")
    io.printTime(timeDataLoaded, timeEnd, "allComputation")
    io.printStep(step)
    io.printStat(reduceInputMessageNumber, "reduceInputMessageNumber")
    io.printStat(reduceInputSize, "reduceInputSize")
    io.printFileEnd(property.appName)

    io.printAllStat(property.algorithmName,
      property.dataset,
      property.sparkPartition,
      step,
      (timeEnd - timeBegin),
      (timeEnd - timeSparkLoaded),
      (timeEnd - timeDataLoaded),
      reduceInputMessageNumber,
      reduceInputSize,
      getCCNumber(rdd),
      getCCNumberNoIsolatedVertices(rdd),
      getCCMaxSize(rdd),
      property.customColumnValue,
      bitmaskCustom)

    if (property.printCCDistribution) {
      io.printCCDistribution(rdd)
      io.printCC(rdd)
    }
  }

  def testEndedComparable(rdd: RDD[(String, Int)],
                          step: Int,
                          timeBegin: Long,
                          timeEnd: Long,
                          timeSparkLoaded: Long,
                          timeDataLoaded: Long,
                          reduceInputMessageNumber: Long,
                          reduceInputSize: Long,
                          bitmaskCustom: String = "000") = {
    io.printTime(timeBegin, timeEnd, "all")
    io.printTime(timeSparkLoaded, timeEnd, "allComputationAndLoadingGraph")
    io.printTime(timeDataLoaded, timeEnd, "allComputation")
    io.printStep(step)
    io.printStat(reduceInputMessageNumber, "reduceInputMessageNumber")
    io.printStat(reduceInputSize, "reduceInputSize")
    io.printFileEnd(property.appName)

    io.printAllStat(property.algorithmName,
      property.dataset,
      property.sparkPartition,
      step,
      (timeEnd - timeBegin),
      (timeEnd - timeSparkLoaded),
      (timeEnd - timeDataLoaded),
      reduceInputMessageNumber,
      reduceInputSize,
      getCCNumberString(rdd),
      getCCNumberNoIsolatedVerticesString(rdd),
      getCCMaxSizeString(rdd),
      property.customColumnValue,
      bitmaskCustom)

    if (property.printCCDistribution) {
      io.printCCDistributionString(rdd)
      //    		io.printCC(rdd)
    }
  }

  def testEndedDensity(rdd: RDD[(Long, Int)],
                       step: Int,
                       timeBegin: Long,
                       timeEnd: Long,
                       timeSparkLoaded: Long,
                       timeDataLoaded: Long,
                       epsilon: Double,
                       k: Int,
                       kMax: Int,
                       randomRestart: Int) = {
    io.printTime(timeBegin, timeEnd, "all")
    io.printTime(timeSparkLoaded, timeEnd, "allComputationAndLoadingGraph")
    io.printTime(timeDataLoaded, timeEnd, "allComputation")
    io.printStep(step)
    io.printFileEnd(property.appName)

    io.printAllStatDensity(property.algorithmName,
      property.dataset,
      property.sparkPartition,
      step,
      (timeEnd - timeBegin),
      (timeEnd - timeSparkLoaded),
      (timeEnd - timeDataLoaded),
      getCCNumber(rdd),
      getCCNumberNoIsolatedVertices(rdd),
      getCCMaxSize(rdd),
      getCCMaxSizeStringNotNoise(rdd),
      property.customColumnValue,
      epsilon,
      k,
      kMax,
      randomRestart)

    if (property.printCCDistribution) {
      io.printCCDistribution(rdd)
      //          io.printCC(rdd)
    }
  }

  def testEndedArray(rdd: Array[(Long, Int)], step: Int, timeBegin: Long, timeEnd: Long, timeSparkLoaded: Long, timeDataLoaded: Long, reduceInputMessageNumber: Long, reduceInputSize: Long) = {
    io.printTime(timeBegin, timeEnd, "all")
    io.printTime(timeSparkLoaded, timeEnd, "allComputationAndLoadingGraph")
    io.printTime(timeDataLoaded, timeEnd, "allComputation")
    io.printStep(step)
    io.printStat(reduceInputMessageNumber, "reduceInputMessageNumber")
    io.printStat(reduceInputSize, "reduceInputSize")
    io.printFileEnd(property.appName)

    io.printAllStat(property.algorithmName,
      property.dataset,
      property.sparkPartition,
      step,
      (timeEnd - timeBegin),
      (timeEnd - timeSparkLoaded),
      (timeEnd - timeDataLoaded),
      reduceInputMessageNumber,
      reduceInputSize,
      getCCNumber(rdd),
      getCCNumberNoIsolatedVertices(rdd),
      getCCMaxSize(rdd),
      property.customColumnValue)

    //    	if(property.printCCDistribution)
    //    		io.printCCDistribution(rdd)
  }

  def testEndedArray(step: Int, timeBegin: Long, timeEnd: Long, timeSparkLoaded: Long, timeDataLoaded: Long, reduceInputMessageNumber: Long, reduceInputSize: Long) = {
    io.printTime(timeBegin, timeEnd, "all")
    io.printTime(timeSparkLoaded, timeEnd, "allComputationAndLoadingGraph")
    io.printTime(timeDataLoaded, timeEnd, "allComputation")
    io.printStep(step)
    io.printStat(reduceInputMessageNumber, "reduceInputMessageNumber")
    io.printStat(reduceInputSize, "reduceInputSize")
    io.printFileEnd(property.appName)

    io.printAllStat(property.algorithmName,
      property.dataset,
      property.sparkPartition,
      step,
      (timeEnd - timeBegin),
      (timeEnd - timeSparkLoaded),
      (timeEnd - timeDataLoaded),
      reduceInputMessageNumber,
      reduceInputSize,
      0,
      0,
      0,
      property.customColumnValue)

    //    	if(property.printCCDistribution)
    //    		io.printCCDistribution(rdd)
  }

  def testEnded(ccNumber: Long, ccNumberNoIsolatedVertices: Long, step: Int, timeBegin: Long, timeEnd: Long, timeSparkLoaded: Long, timeDataLoaded: Long, reduceInputMessageNumber: Long, reduceInputSize: Long) = {
    io.printTime(timeBegin, timeEnd, "all")
    io.printTime(timeSparkLoaded, timeEnd, "allComputationAndLoadingGraph")
    io.printTime(timeDataLoaded, timeEnd, "allComputation")
    io.printStep(step)
    io.printStat(reduceInputMessageNumber, "reduceInputMessageNumber")
    io.printStat(reduceInputSize, "reduceInputSize")
    io.printFileEnd(property.appName)

    io.printAllStat(property.algorithmName,
      property.dataset,
      property.sparkPartition,
      step,
      (timeEnd - timeBegin),
      (timeEnd - timeSparkLoaded),
      (timeEnd - timeDataLoaded),
      reduceInputMessageNumber,
      reduceInputSize,
      ccNumber,
      ccNumberNoIsolatedVertices,
      0,
      property.customColumnValue)
  }
}
