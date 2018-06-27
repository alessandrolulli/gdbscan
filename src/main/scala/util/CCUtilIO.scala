/*
 * Copyright (C) 2011-2012 the original author or authors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package util

import java.io.FileWriter

import enn.densityBased.ENNConfig
import org.apache.spark.rdd.RDD

class CCUtilIO(property: CCPropertiesImmutable) extends Serializable {
  val fileStatDescription = "algorithmName,dataset,partition,step,timeAll,timeLoadingAndComputation,timeComputation,reduceInputMessageNumber,reduceInputSize,ccNumber,ccNumberNoIsolatedVertices,ccMaxSize,customColumn,cores,switchLocal,shuffleManager,compressionCodec,bitmaskCustom,sparkShuffleConsolidateFiles,edgeThreshold"
  val fileSimplificationDescritpion = "dataset,step,activeVertices,activeVerticesNormalized,algorithmName,activeEdges,degreeAvg,degreeMax"
  val fileTimeStep = "dataset,algorithmName,step,time,cores,switchLocal,shuffleManager,compressionCodec,bitmaskCustom,sparkShuffleConsolidateFiles"

  val fileStatDescriptionDiameter = "algorithmName,dataset,partition,step,timeAll,timeLoadingAndComputation,timeComputation,reduceInputMessageNumber,reduceInputSize,diameter,customColumn,cores,switchLocal,shuffleManager,compressionCodec,sparkShuffleConsolidateFiles,selfFunction,candidateFunction,loadBalancing,selfStar,transmitPreviousNeighbours,stepAll,diameterPlus"
  val fileSimplificationDescritpionDiameter = "dataset,step,activeVertices,activeVerticesNormalized,algorithmName,activeEdges,degreeAvg,degreeMax,selfFunction,candidateFunction,loadBalancing,selfStar,transmitPreviousNeighbours"
  val fileTimeStepDiameter = "dataset,algorithmName,step,time,cores,switchLocal,shuffleManager,compressionCodec,bitmaskCustom,sparkShuffleConsolidateFiles,selfFunction,candidateFunction,loadBalancing,selfStar,transmitPreviousNeighbours"

  def printStat(data: Long, description: String): Int =
    {
      printStat(data.toString, description)
    }

  def printStat(data: Double, description: String): Int =
    {
      printStat(data.toString, description)
    }

  def printStat(data: String, description: String): Int =
    {
      val printFile = new FileWriter("time.txt", true)
      printFile.write(description + ": " + data + "\n")
      printFile.close

      0
    }
  
  def printData(file : String, data: String*): Int =
    {
      val printFile = new FileWriter(file, true)
      printFile.write(data.mkString(",") + "\n")
      printFile.close

      0
    }

  def printSimplification(step: Int, activeVertices: Long, initialVertices: Long, activeEdges: Double, degreeMax: Int) =
    {
      val printFile = new FileWriter("simplification.txt", true)

      val token: Array[Object] = Array(property.dataset,
        step.toString,
        activeVertices.toString,
        ((((activeVertices.toDouble * 100) / initialVertices) * 100).round.toDouble / 100).toString,
        property.algorithmName,
        activeEdges.toString,
        (activeEdges / activeVertices).toString,
        degreeMax.toString)
      printFile.write(token.mkString(",") + "\n")

      printFile.close
    }

  def printTimeStep(step: Int, time: Long) =
    {
      val printFile = new FileWriter("timeStep.txt", true)

      // dataset, algorithmName, step, time
      val token: Array[Object] = Array(property.dataset,
        property.algorithmName,
        step.toString,
        time.toString,
        property.sparkCoresMax.toString,
        property.switchLocal.toString,
        property.sparkShuffleManager,
        property.sparkCompressionCodec)
      printFile.write(token.mkString(",") + "\n")

      printFile.close
    }

  def printMessageStep(step: Int, messageNumber: Long, messageSize: Long, bitmaskCustom: String = "000") =
    {
      val printFile = new FileWriter("messageStep.txt", true)

      val token: Array[Object] = Array(property.dataset, property.algorithmName, step.toString, messageNumber.toString, messageSize.toString, bitmaskCustom, property.sparkShuffleConsolidateFiles)
      printFile.write(token.mkString(",") + "\n")

      printFile.close
    }

  def printStatSimple(
    value: String) =
    {
      val printFile = new FileWriter("stats.txt", true)

      val token: Array[Object] = Array(property.algorithmName,
        property.dataset,
        value)

      printFile.write(token.mkString(",") + "\n")
      printFile.close
    }

  def printCommonStat(
    step: Int,
    timaAll: Long,
    timeLoadingAndComputation: Long,
    timeComputation: Long,
    reduceInputMessageNumber: Long,
    reduceInputSize: Long,
    iteration: Int) =
    {
      val printFile = new FileWriter("stats.txt", true)

      val desc = "algorithmName,dataset,partition,step,timeAll,timeGraph,timeComputation,messageNumber,messageSize,customColumn,cores,shuffleManager,compression,consolidateFiles,iteration"

      val token: Array[Object] = Array(property.algorithmName,
        property.dataset,
        property.sparkPartition.toString,
        step.toString,
        timaAll.toString,
        timeLoadingAndComputation.toString,
        timeComputation.toString,
        reduceInputMessageNumber.toString,
        reduceInputSize.toString,
        property.customColumnValue,
        property.sparkCoresMax.toString,
        property.sparkShuffleManager,
        property.sparkCompressionCodec,
        property.sparkShuffleConsolidateFiles,
        iteration.toString)

      printFile.write(token.mkString(",") + "\n")
      printFile.close
    }

  def printStatENN(
    maxIterations: Int,
    timaAll: Long,
    k: Int,
    kMax: Int,
    epsilon: Double,
    randomRestart: Int,
    printStep: Int,
    totalNode: Long,
    computingNodes: Double,
    stoppedNodes: Double,
    performance: Boolean,
    messageNumber: Long,
    config: ENNConfig) =
    {
      val printFile = new FileWriter("stats.txt", true)

      val desc = "algorithmName,dataset,partition,maxIterations,timeAll,customColumn,cores,shuffleManager,compression,consolidateFiles,k,kMax,epsilon,randomRestart,printingOutput,totalNodes,computingNodes,performance,messageNumber,numberOfComparison"

      val token: Array[Object] = Array(property.algorithmName,
        property.dataset,
        property.sparkPartition.toString,
        maxIterations.toString,
        timaAll.toString,
        property.customColumnValue,
        property.sparkCoresMax.toString,
        property.sparkShuffleManager,
        property.sparkCompressionCodec,
        property.sparkShuffleConsolidateFiles,
        k.toString,
        kMax.toString,
        epsilon.toString,
        randomRestart.toString,
        { if (printStep < 0) false else true }.toString,
        totalNode.toString,
        computingNodes.toString,
        stoppedNodes.toString(),
        performance.toString,
        messageNumber.toString,
        config.neigbourAggregation,
        config.excludeNode.toString,
        config.dimensionLimit.toString,
        config.sampling.toString)

      printFile.write(token.mkString(",") + "\n")
      printFile.close
    }

  def printAllStat(algorithmName: String,
                   dataset: String,
                   partition: Int,
                   step: Int,
                   timaAll: Long,
                   timeLoadingAndComputation: Long,
                   timeComputation: Long,
                   reduceInputMessageNumber: Long,
                   reduceInputSize: Long,
                   ccNumber: Long,
                   ccNumberNoIsolatedVertices: Long,
                   ccMaxSize: Int,
                   customColumnValue: String,
                   bitmaskCustom: String = "000") =
    {
      val printFile = new FileWriter("stats.txt", true)

      val token: Array[Object] = Array(algorithmName,
        dataset,
        partition.toString,
        step.toString,
        timaAll.toString,
        timeLoadingAndComputation.toString,
        timeComputation.toString,
        reduceInputMessageNumber.toString,
        reduceInputSize.toString,
        ccNumber.toString,
        ccNumberNoIsolatedVertices.toString,
        ccMaxSize.toString,
        customColumnValue,
        property.sparkCoresMax.toString,
        property.switchLocal.toString,
        property.sparkShuffleManager,
        property.sparkCompressionCodec,
        bitmaskCustom,
        property.sparkShuffleConsolidateFiles,
        property.edgeThreshold.toString,
        property.coreThreshold.toString)

      printFile.write(token.mkString(",") + "\n")
      printFile.close
    }

  def printAllStatDensity(algorithmName: String,
                          dataset: String,
                          partition: Int,
                          step: Int,
                          timaAll: Long,
                          timeLoadingAndComputation: Long,
                          timeComputation: Long,
                          ccNumber: Long,
                          ccNumberNoIsolatedVertices: Long,
                          ccMaxSize: Int,
                          ccMaxSizeNotNoise: Int,
                          customColumnValue: String,
                          epsilon: Double,
                          k: Int,
                          kMax: Int,
                          randomRestart: Int) =
    {
      val printFile = new FileWriter("stats.txt", true)

      val desc = "algorithmName,dataset,partition,step,timaAll,timeLoadingAndComputation,timeComputation,ccNumber,ccNumberNoIsolatedVertices,ccMaxSize,ccMaxSizeNotNoise,customColumnValue,sparkCoresMax,epsilon,coreThreshold,k,kMax,randomRestart"

      val token: Array[Object] = Array(algorithmName,
        dataset,
        partition.toString,
        step.toString,
        timaAll.toString,
        timeLoadingAndComputation.toString,
        timeComputation.toString,
        ccNumber.toString,
        ccNumberNoIsolatedVertices.toString,
        ccMaxSize.toString,
        ccMaxSizeNotNoise.toString,
        customColumnValue,
        property.sparkCoresMax.toString,
        epsilon.toString,
        property.coreThreshold.toString,
        k.toString,
        kMax.toString,
        randomRestart.toString)

      printFile.write(token.mkString(",") + "\n")
      printFile.close
    }

  def printCCDistribution(rdd: RDD[(Long, Int)]) =
    {
      val printFile = new FileWriter("distribution.txt", true)

      val ccDistribution = rdd.map(t => (t._2, 1)).reduceByKey { case (a, b) => a + b }.map(t => t._1 + "," + t._2 + "\n").reduce { case (a, b) => a + b }

      //		val token : Array[Object] = Array(algorithmName, dataset, partition.toString, hybridMessageSizeBound.toString, step.toString, timaAll.toString, timeLoadingAndComputation.toString, timeComputation.toString, reduceInputMessageNumber.toString, reduceInputSize.toString, ccNumber.toString, ccMaxSize.toString)
      //		
      //		printFile.write(joiner.join(token)+ "\n" )
      printFile.write(ccDistribution + "\n")

      printFile.close
    }

  def printCCDistributionString(rdd: RDD[(String, Int)]) =
    {
      val printFile = new FileWriter("distribution.txt", true)

      val ccDistribution = rdd.map(t => (t._2, 1)).reduceByKey { case (a, b) => a + b }.map(t => property.dataset + "," + t._1 + "," + t._2 + "," + property.edgeThreshold.toString + "\n").reduce { case (a, b) => a + b }

      //		val token : Array[Object] = Array(algorithmName, dataset, partition.toString, hybridMessageSizeBound.toString, step.toString, timaAll.toString, timeLoadingAndComputation.toString, timeComputation.toString, reduceInputMessageNumber.toString, reduceInputSize.toString, ccNumber.toString, ccMaxSize.toString)
      //		
      //		printFile.write(joiner.join(token)+ "\n" )
      printFile.write(ccDistribution + "\n")

      printFile.close
    }

  def printClusteringQuality(
    k: Int,
    iteration: Int,
    minimalInterClusterDistance: Double,
    maximalIntraClusterDistance: Double,
    separation: Double,
    compactness: Double,
    silhoutte: Double,
    validClusterNumber: Int) =
    {
      val printFile = new FileWriter("stats.txt", true)

      // description = algorithmName,dataset,dataset2,custom,k,iteration,edgeThreshold,minimalInterClusterDistance,maximalIntraClusterDistance,dunn,separation,compactness,silhoutte,validClusterNumber
      val token: Array[Object] = Array(property.algorithmName,
        property.dataset,
        property.dataset2,
        property.customColumnValue,
        k.toString,
        iteration.toString,
        property.edgeThreshold.toString,
        minimalInterClusterDistance.toString,
        maximalIntraClusterDistance.toString,
        (minimalInterClusterDistance / maximalIntraClusterDistance).toString,
        separation.toString,
        compactness.toString,
        silhoutte.toString,
        validClusterNumber.toString)

      printFile.write(token.mkString(",") + "\n")
      printFile.close
    }

  def printCC(rdd: RDD[(Long, Int)]) =
    {
      val printFile = new FileWriter("cc.txt", true)

      val ccDistribution = rdd.map(t => t._1 + "," + t._2 + "\n").reduce { case (a, b) => a + b }

      //		val token : Array[Object] = Array(algorithmName, dataset, partition.toString, hybridMessageSizeBound.toString, step.toString, timaAll.toString, timeLoadingAndComputation.toString, timeComputation.toString, reduceInputMessageNumber.toString, reduceInputSize.toString, ccNumber.toString, ccMaxSize.toString)
      //		
      //		printFile.write(joiner.join(token)+ "\n" )
      printFile.write(ccDistribution + "\n")

      printFile.close
    }

  def printEdgelist(data: RDD[(Long, Long)]) =
    {
      val collected = data.collect.iterator
      val printFile = new FileWriter("edgelist.txt", true)
      while (collected.hasNext) {
        val next = collected.next
        printFile.write(next._1 + " " + next._2 + "\n")
      }
      printFile.close
    }

  def printFileStart(description: String) =
    {
      val printFile = new FileWriter("time.txt", true)
      printFile.write("\n" + description + ": START\n")
      printFile.close
    }

  def printFileEnd(description: String) =
    {
      val printFile = new FileWriter("time.txt", true)
      printFile.write(description + ": END\n")
      printFile.close
    }

  def printTime(start: Long, end: Long, description: String) =
    {
      val printFile = new FileWriter("time.txt", true)
      printFile.write(description + ": " + (end - start) + "\n")
      printFile.close
    }

  def printStep(step: Int) =
    {
      val printFile = new FileWriter("time.txt", true)
      printFile.write("step: " + step + "\n")
      printFile.close
    }

  def printTimeStep(start: Long, red: Long, end: Long) =
    {
      val printFile = new FileWriter("time.txt", true)
      printFile.write("blue: " + (red - start) + " red: " + (end - red) + " all: " + (end - start) + "\n")
      printFile.close
    }

  def printToFile(file: String, data: String) =
    {
      val printFile = new FileWriter(file, true)
      printFile.write(data)
      printFile.close
    }

}