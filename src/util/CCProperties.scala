package util

import java.util.Properties
import java.io.InputStream
import java.io.FileInputStream


class CCProperties(algorithmName: String, configurationFile : String) extends Serializable
{
	val property = new Properties
	
	def load() : CCProperties =
	{
		var input : InputStream = null
 
		input = new FileInputStream(configurationFile);
 
		property.load(input);
		
		this
	}
	
	def get(data : String, default : String) =
	{
		property.getProperty(data, default)
	}
	
	def getBoolean(data : String, default : Boolean) =
	{
		get(data, default.toString).toBoolean
	}
	
	def getInt(data : String, default : Int) =
	{
		get(data, default.toString).toInt
	}
	
	def getDouble(data : String, default : Double) =
	{
		get(data, default.toString).toDouble
	}
	
	def getImmutable : CCPropertiesImmutable =
	{
		val dataset = get("dataset", "")
		val dataset2 = get("dataset2", "")
		val jarPath = get("jarPath", "")
		val sparkMaster = get("sparkMaster", "local[2]")
		val sparkExecutorMemory = get("sparkExecutorMemory", "14g")
		val sparkPartition = get("sparkPartition", "32").toInt
		val sparkBlockManagerSlaveTimeoutMs= get("sparkBlockManagerSlaveTimeoutMs", "45000")
		val sparkCoresMax = get("sparkCoresMax", "-1").toInt
		val sparkAkkaFrameSize = get("sparkAkkaFrameSize", "100").toString
		val sparkShuffleManager = get("sparkShuffleManager", "SORT").toString
		val sparkCompressionCodec = get("sparkCompressionCodec", "snappy").toString
		val sparkShuffleConsolidateFiles = get("sparkShuffleConsolidateFiles", "false").toString
		val sparkDriverMaxResultSize = get("sparkDriverMaxResultSize", "1g").toString
		var separator = get("edgelistSeparator", "space")
		if(separator.equals("space")) separator = " "
		val printMessageStat = get("printMessageStat", "false").toBoolean
		val printLargestCC = get("printLargestCC", "false").toBoolean
		val printCC = get("printCC", "false").toBoolean
		val printCCDistribution = get("printCCDistribution", "false").toBoolean
		val printAll = get("printAll", "false").toBoolean
		val customColumnValue = get("customColumnValue", "")
		val algorithmNameFromConfiguration = get("algorithmName", algorithmName)
		val switchLocal = get("switchLocal", "-1").toInt
		val switchLocalActive = switchLocal != -1 
		val vertexIdMultiplier = get("vertexIdMultiplier", "-1").toInt
		val loadBalancing = get("loadBalancing", "false").toBoolean
		val vertexNumber = get("vertexNumber", "-1").toInt
		val outputFile = get("outputFile", "")
		val coreThreshold = getInt("coreThreshold", 10)
		
		//############# WITH YARN
		val sparkExecutorInstances = get("sparkExecutorInstances", "-1").toInt
		
		//################## DIAMETER
		
		val selfStar = get("selfStar", "true").toBoolean
		val transmitPreviousNeighbours = get("transmitPreviousNeighbours", "true").toBoolean
		val edgeThreshold = getDouble("edgeThreshold", -1)
		
		new CCPropertiesImmutable(	algorithmNameFromConfiguration, 
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