package crackerDensity

import scala.Array.canBuildFrom
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import util.CCProperties
import util.CCUtil
import util.CCPropertiesImmutable
import java.io.PrintWriter
import java.io.File
import java.io.FileWriter
import org.apache.spark.storage.StorageLevel

object CrackerAllComparable {

  def mainGO(ennGraph: String, args: Array[String]): Unit =
    {
      val DEFAULT_STORAGE_LEVEL = StorageLevel.MEMORY_AND_DISK
      val timeBegin = System.currentTimeMillis()

      val propertyLoad = new CCProperties("CRACKER_DENSITY", args(0)).load
      val crackerUseUnionInsteadOfJoin = propertyLoad.getBoolean("crackerUseUnionInsteadOfJoin", true)
      val crackerCoalescePartition = propertyLoad.getBoolean("crackerCoalescePartition", true)
      val crackerForceEvaluation = propertyLoad.getBoolean("crackerForceEvaluation", true)

      val k = propertyLoad.getInt("k", 5);
      val kMax = propertyLoad.getInt("kMax", 20);
      val epsilon = propertyLoad.getDouble("epsilon", 0.9);
      val maxIterations = propertyLoad.getInt("maxIterations", 5);
      val randomRestart = propertyLoad.getInt("randomRestart", 5);

      val property = propertyLoad.getImmutable
      val cracker = new CrackerAlgorithm(property)

      val util = new CCUtil(property)
      val spark = util.getSparkContext()

      val timeSparkLoaded = System.currentTimeMillis()
      val file = spark.textFile(ennGraph, property.sparkPartition)

      util.io.printFileStart(property.appName)

      val begin = util.loadEdgeFromFileAdjComparableDegree(file, cracker)
      var ret = begin

      val timeDataLoaded = System.currentTimeMillis()

      var control = false;
      var step = 0

      var treeRDD: Option[RDD[(Long, CrackerTreeMessageTree)]] = Option.empty

      treeRDD = Option.apply(ret.map(t => (t._1, new CrackerTreeMessageTree(Option.empty, Set(), { if (t._2.selfDegree >= property.coreThreshold) 1 else 0 }))))

      def forceLoadBalancing(step: Int): Boolean =
        {
          step == 0 || step == 2 || step == 8 || step == 16 || step == 32
          //				step < 10 && step % 3 == 0
        }

      while (!control) {
        // simplification step
        val timeStepStart = System.currentTimeMillis()

        ret = ret.flatMap(item => cracker.emitBlue(item, property.coreThreshold))

        ret = ret.reduceByKey(cracker.reduceBlue).persist(DEFAULT_STORAGE_LEVEL)

        val active = ret.count
        control = active <= 0

        val timeStepBlue = System.currentTimeMillis()
        util.printTimeStep(step + 1, timeStepBlue - timeStepStart)

        if (!control) {
          // reduction step
          val check = step

          //                    util.io.printStat(ret.filter(t => t._2.selfDegree < 0).count, "NOT VALID RED")
          val tmp = ret.flatMap(item => cracker.emitRed(item, forceLoadBalancing(check), property.coreThreshold))

          val tmpReduced = tmp.reduceByKey(cracker.reduceRed)

          ret = tmpReduced.filter(t => t._2.first.isDefined).map(t => (t._1, t._2.first.get))
          treeRDD = cracker.mergeTree(treeRDD, tmpReduced.filter(t => t._2.second.isDefined).map(t => (t._1, t._2.second.get)), crackerUseUnionInsteadOfJoin, crackerForceEvaluation)

          val timeStepEnd = System.currentTimeMillis()
          step = step + 2
          util.io.printTimeStep(timeStepStart, timeStepBlue, timeStepEnd)
          util.printTimeStep(step, timeStepEnd - timeStepBlue)
        } else {
          step = step + 1
          util.io.printTime(timeStepStart, timeStepBlue, "blue")
        }
      }

      var treeRDDPropagationTmp = treeRDD.get

      if (crackerUseUnionInsteadOfJoin && crackerCoalescePartition) {
        val timeStepStart = System.currentTimeMillis()
        treeRDDPropagationTmp = treeRDDPropagationTmp.coalesce(property.sparkPartition)
        val timeStepBlue = System.currentTimeMillis()
        util.io.printTime(timeStepStart, timeStepBlue, "coalescing")
      }

      var treeRDDPropagation = treeRDDPropagationTmp.reduceByKey(cracker.reducePrepareDataForPropagation).map(t => (t._1, t._2.getMessagePropagation(t._1))).persist(DEFAULT_STORAGE_LEVEL)
      //            .filter(t => !t._2.min.isDefined || (t._2.min.isDefined && !t._2.child.isEmpty))
      control = false
      while (!control) {
        val timeStepStart = System.currentTimeMillis()
        treeRDDPropagation = treeRDDPropagation.flatMap(item => cracker.mapPropagate(item))

        treeRDDPropagation = treeRDDPropagation.reduceByKey(cracker.reducePropagate).persist(DEFAULT_STORAGE_LEVEL)
        control = treeRDDPropagation.map(t => t._2.min.isDefined).reduce { case (a, b) => a && b }

        step = step + 1
        val timeStepBlue = System.currentTimeMillis()
        util.io.printTime(timeStepStart, timeStepBlue, "propagation")
        util.printTimeStep(step, timeStepBlue - timeStepStart)
      }

      val timeEnd = System.currentTimeMillis()

      util.testEndedDensity(treeRDDPropagation.map(t => (t._2.min.get, 1)).reduceByKey { case (a, b) => a + b },
        step,
        timeBegin,
        timeEnd,
        timeSparkLoaded,
        timeDataLoaded,
        epsilon,
        k,
        kMax,
        randomRestart)

      if (property.printCC) {
        val toPrint = treeRDDPropagation.map(t => t._1 + "\t" + t._2.min.get + "\t" + t._2.core)
        toPrint.coalesce(1, true).saveAsTextFile(property.outputFile + "_CLUSTERING")
      }

    }
}