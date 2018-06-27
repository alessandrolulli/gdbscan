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

import java.io.{FileInputStream, InputStream}
import java.util.Properties


class CCProperties(algorithmName: String, configurationFile: String) extends Serializable {
  val property = new Properties

  def load(): CCProperties = {
    var input: InputStream = null

    input = new FileInputStream(configurationFile);

    property.load(input);

    this
  }

  def get(data: String, default: String) = {
    property.getProperty(data, default)
  }

  def getBoolean(data: String, default: Boolean) = {
    get(data, default.toString).toBoolean
  }

  def getInt(data: String, default: Int) = {
    get(data, default.toString).toInt
  }

  def getDouble(data: String, default: Double) = {
    get(data, default.toString).toDouble
  }

  def getImmutable: CCPropertiesImmutable = {
    //############# WITH YARN
    val sparkExecutorInstances = get("sparkExecutorInstances", "-1").toInt

    val dataset = get("dataset", "")
    val dataset2 = get("dataset2", "")
    val jarPath = get("jarPath", "")
    val sparkMaster = get("sparkMaster", "")
    val sparkExecutorMemory = get("sparkExecutorMemory", "14g")
    val sparkCoresMax = get("sparkCoresMax", "-1").toInt
    val sparkPartition = getInt("sparkPartition", Math.max(sparkCoresMax * 4, sparkExecutorInstances * 4))
    val sparkBlockManagerSlaveTimeoutMs = get("sparkBlockManagerSlaveTimeoutMs", "500000")
    val sparkAkkaFrameSize = get("sparkAkkaFrameSize", "100").toString
    val sparkShuffleManager = get("sparkShuffleManager", "SORT").toString
    val sparkCompressionCodec = get("sparkCompressionCodec", "lz4").toString
    val sparkShuffleConsolidateFiles = get("sparkShuffleConsolidateFiles", "false").toString
    val sparkDriverMaxResultSize = get("sparkDriverMaxResultSize", "1g").toString
    var separator = get("edgelistSeparator", get("separator", "space"))
    if (separator.equals("space")) separator = " "
    val printMessageStat = get("printMessageStat", "false").toBoolean
    val printLargestCC = get("printLargestCC", "false").toBoolean
    val printCC = get("printCC", "true").toBoolean
    val printCCDistribution = get("printCCDistribution", "false").toBoolean
    val printAll = get("printAll", "false").toBoolean
    val customColumnValue = get("customColumnValue", "DENSITY")
    val algorithmNameFromConfiguration = get("algorithmName", algorithmName)
    val switchLocal = get("switchLocal", "-1").toInt
    val switchLocalActive = switchLocal != -1
    val vertexIdMultiplier = get("vertexIdMultiplier", "-1").toInt
    val loadBalancing = get("loadBalancing", "false").toBoolean
    val vertexNumber = get("vertexNumber", "-1").toInt
    val outputFile = get("outputFile", "")
    val coreThreshold = getInt("coreThreshold", 10)

    //################## DIAMETER

    val selfStar = get("selfStar", "true").toBoolean
    val transmitPreviousNeighbours = get("transmitPreviousNeighbours", "true").toBoolean
    val edgeThreshold = getDouble("edgeThreshold", -1)

    new CCPropertiesImmutable(algorithmNameFromConfiguration,
      dataset,
      dataset2,
      outputFile,
      jarPath,
      sparkMaster,
      sparkPartition,
      sparkExecutorMemory,
      sparkBlockManagerSlaveTimeoutMs,
      sparkCoresMax,
      sparkShuffleManager,
      sparkCompressionCodec,
      sparkShuffleConsolidateFiles,
      sparkAkkaFrameSize,
      sparkDriverMaxResultSize,
      sparkExecutorInstances,
      separator,
      printMessageStat,
      printLargestCC,
      printCC,
      printCCDistribution,
      printAll,
      customColumnValue,
      switchLocal,
      switchLocalActive,
      vertexIdMultiplier,
      vertexNumber,
      loadBalancing,
      selfStar,
      transmitPreviousNeighbours,
      edgeThreshold,
      coreThreshold)
  }
}