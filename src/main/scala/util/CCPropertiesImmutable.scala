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

class CCPropertiesImmutable(algorithmNameFromConfig : String, 
							val dataset : String, 
							val dataset2 : String,
							val outputFile : String,
							val jarPath : String, 
							val sparkMaster : String,
							val sparkPartition : Int,
							val sparkExecutorMemory : String, 
							val sparkBlockManagerSlaveTimeoutMs : String,
							val sparkCoresMax : Int,
							val sparkShuffleManager : String,
							val sparkCompressionCodec : String,
							val sparkShuffleConsolidateFiles : String,
							val sparkAkkaFrameSize : String,
							val sparkDriverMaxResultSize : String,
							val sparkExecutorInstances : Int,
							val separator : String,
							val printMessageStat : Boolean,
							val printLargestCC : Boolean,
							val printCC : Boolean,
							val printCCDistribution : Boolean,
							val printAll : Boolean,
							val customColumnValue : String,
							val switchLocal : Int,
							val switchLocalActive : Boolean,
							val vertexIdMultiplier : Int,
							val vertexNumber : Int,
							val loadBalancing : Boolean,
							val selfStar : Boolean,
							val transmitPreviousNeighbours : Boolean,
							val edgeThreshold : Double,
							val coreThreshold : Int) extends Serializable
{
    val algorithmName = if(loadBalancing) algorithmNameFromConfig+"_LOAD" else algorithmNameFromConfig
	val appName = algorithmName+"_"+dataset
	val allStat = printMessageStat && appName.contains("CRA")
	val filenameLargestCC = dataset+"_largestCC"
}