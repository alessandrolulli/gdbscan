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

package enn.densityBased

import enn.densityBased.init.{ENNInitCircle, ENNInitRandom, ENNInitSystematicSampling}
import util.{CCProperties, CCPropertiesImmutable, CCUtil}

/**
  * @author alemare
  */
class ENNConfig(args_ : Array[String], appName: String = "ENN") extends Serializable {
  val propertyLoad : CCProperties= new CCProperties(appName, args_(0)).load()
  val property : CCPropertiesImmutable = propertyLoad.getImmutable

  val k = propertyLoad.getInt("k", 5)
  val kMax = propertyLoad.getInt("kMax", 20)
  val epsilon = propertyLoad.getDouble("epsilon", 0.9)
  val alpha = propertyLoad.getDouble("alpha", 0.4)
  val addNeighborProbability = propertyLoad.getDouble("addNeighborProbability", 1.0)
  val maxIterations = propertyLoad.getInt("maxIterations", 5)
  val maxComputingNodes = propertyLoad.getInt("maxComputingNodes", 500000)
  val randomRestart = propertyLoad.getInt("randomRestart", 5)
  val printStep = propertyLoad.getInt("printStep", 1)

  val (groundtruth, groundtruthAvailable): (String, Boolean) = propertyLoad.get("groundtruth", "NO") match {
    case "NO" => ("NO", false)
    case "SAME" => (property.dataset, true)
    case default => (default, true)
  }

  val ennSkip = propertyLoad.getBoolean("ennSkip", false)
  val knnSkip = propertyLoad.getBoolean("knnSkip", false)
  val clusterSkip = propertyLoad.getBoolean("clusterSkip", false)
  val internalEvaluationSkip = propertyLoad.getBoolean("internalEvaluationSkip", true)
  val externalEvaluationSkip = propertyLoad.getBoolean("externalEvaluationSkip", false) || !groundtruthAvailable
  val skipENN = ennSkip
  val skipCluster = clusterSkip
  val skipInternalEvaluation = internalEvaluationSkip
  val skipExternalEvaluation = externalEvaluationSkip

  val clusterMinSize = propertyLoad.getInt("clusterMinSize", 100)
  val printOutput = propertyLoad.getBoolean("printOutput", true)

  val terminationActiveNodes = propertyLoad.getDouble("terminationActiveNodes", 0.1)
  val terminationRemovedNodes = propertyLoad.getDouble("terminationRemovedNodes", 0.0001)
  val sampling = propertyLoad.getInt("sampling", -1)

  val columnDataA = propertyLoad.getInt("columnDataA", 2)
  val columnDataB = propertyLoad.getInt("columnDataB", 3)

  val neigbourAggregation = propertyLoad.get("neigbourAggregation", "normal")
  val outputFileCluster = propertyLoad.get("outputFileCluster", "")
  val excludeNode = propertyLoad.getBoolean("excludeNode", false)
  val filterSparse = propertyLoad.getBoolean("filterSparse", false)

  val performance = propertyLoad.getBoolean("performance", true)
  val messageStat = propertyLoad.getBoolean("messageStat", false)
  val endIterationValue = if (performance) -1 else -2

  val dimensionLimit = propertyLoad.getInt("dimensionLimit", 2)

  val initPolicy = propertyLoad.get("initPolicy", "RANDOM") match {
    case "RANDOM" => new ENNInitRandom(this)
    case "CIRCLE" => new ENNInitCircle(this)
    case "SYSTEMATIC" => new ENNInitSystematicSampling(this)
    case _ => new ENNInitRandom(this)
  }

  val util = new CCUtil(property)

}