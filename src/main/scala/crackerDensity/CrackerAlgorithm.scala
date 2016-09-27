package crackerDensity

import org.apache.spark.SparkContext._
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import java.io.FileWriter
import util.CCPropertiesImmutable

class CrackerAlgorithm(property : CCPropertiesImmutable) extends Serializable {
	def mapPropagate(item : (Long, CrackerTreeMessagePropagation)) : Iterable[(Long, CrackerTreeMessagePropagation)] =
		{
			var outputList : ListBuffer[(Long, CrackerTreeMessagePropagation)] = new ListBuffer
			if (item._2.min.isDefined) {
				outputList.prepend((item._1, new CrackerTreeMessagePropagation(item._2.min, Set(),  item._2.core)))
				val it = item._2.child.iterator
				while (it.hasNext) {
					val next = it.next
					outputList.prepend((next, new CrackerTreeMessagePropagation(item._2.min, Set(), -1)))
				}
			} else {
				outputList.prepend(item)
			}
			outputList
		}

	def reducePropagate(item1 : CrackerTreeMessagePropagation, item2 : CrackerTreeMessagePropagation) : CrackerTreeMessagePropagation =
		{
			var minEnd = item1.min
			if (minEnd.isEmpty) minEnd = item2.min
            var coreNew = item1.core
            if(coreNew == -1) coreNew = item2.core

			new CrackerTreeMessagePropagation(minEnd, item1.child ++ item2.child, coreNew)
		}

	def emitBlue(item : (Long, CrackerTreeMessageIdentification), coreThreshold : Int) : Iterable[(Long, CrackerTreeMessageIdentification)] =
		{
			var outputList : ListBuffer[(Long, CrackerTreeMessageIdentification)] = new ListBuffer
            
			if (item._2.candidate == item._1 && (item._2.neigh.isEmpty || (item._2.neigh.size == 1 && item._2.neigh.contains(item._1)))) {
				//                outputList.prepend( ( item._1, new CrackerTreeMessage( item._2.min, Set()) ) )
			} else {

				val candidate = item._2.candidate

				outputList.prepend((item._1, new CrackerTreeMessageIdentification(item._2.selfDegree, candidate, item._2.candidateDegree, Map(candidate -> item._2.candidateDegree))))
				
				val it = item._2.neighFilter(coreThreshold).iterator
				while (it.hasNext) {
					val next = it.next
					outputList.prepend((next._1, new CrackerTreeMessageIdentification(next._2, candidate, item._2.candidateDegree, Map(candidate -> item._2.candidateDegree))))
				}
			}
			
//			val printFile = new FileWriter( "check.txt", true )
//
//		printFile.write("BLUE "+item._1+ "\n" )
//		
//        printFile.close

			outputList.toIterable
		}

	def emitRed(item : (Long, CrackerTreeMessageIdentification), forceLoadBalancing : Boolean, coreThreshold : Int) : Iterable[(Long, CrackerTreeMessageRedPhase)] = {

		var outputList : ListBuffer[(Long, CrackerTreeMessageRedPhase)] = new ListBuffer

		val minset : Map[Long, Int] = item._2.neighFilter(coreThreshold)
		if (minset.size > 1) 
		{
			if(property.loadBalancing || forceLoadBalancing)
			{
				outputList.prepend((item._2.candidate, CrackerTreeMessageRedPhase.apply(new CrackerTreeMessageIdentification(-1, item._2.candidate, item._2.candidateDegree, Map(item._2.candidate -> item._2.candidateDegree)))))
			}
		    else
		    {
		        outputList.prepend((item._2.candidate, CrackerTreeMessageRedPhase.apply(new CrackerTreeMessageIdentification(-1, item._2.candidate, item._2.candidateDegree, minset))))
		    }
			
			var it = minset.iterator
			while (it.hasNext) {
				val value : (Long, Int) = it.next
				if (value._1 != item._2.candidate)
					outputList.prepend((value._1, CrackerTreeMessageRedPhase.apply(new CrackerTreeMessageIdentification(value._2, item._2.candidate, item._2.candidateDegree, Map(item._2.candidate -> item._2.candidateDegree)))))
			} 
		} else if (minset.size == 1 && minset.contains(item._1)) {
			outputList.prepend((item._1, CrackerTreeMessageRedPhase.apply(new CrackerTreeMessageIdentification(item._2.selfDegree, item._1, 0, Map()))))
		}

		if (!item._2.neigh.contains(item._1)) {
			outputList.prepend((item._2.candidate, CrackerTreeMessageRedPhase.apply(new CrackerTreeMessageTree(Option.empty, Set(item._1), -1))))
            var core = 0
            if (item._2.selfDegree >= coreThreshold) core = 1
            
			outputList.prepend((item._1, CrackerTreeMessageRedPhase.apply(new CrackerTreeMessageTree(Option.apply(item._2.candidate), Set(), core))))
		} else
		{
		    outputList.prepend((item._1, CrackerTreeMessageRedPhase.apply(new CrackerTreeMessageIdentification(item._2.selfDegree, item._2.candidate, item._2.candidateDegree, Map(item._2.candidate -> item._2.candidateDegree)))))
		}
//					val printFile = new FileWriter( "check.txt", true )
//
//		printFile.write("RED "+item._1+ "\n" )
//		
//        printFile.close

		outputList.toIterable
	}

	def reduceBlue(item1 : CrackerTreeMessageIdentification, item2 : CrackerTreeMessageIdentification) : CrackerTreeMessageIdentification =
	{
    	item1.merge(item2)
	}

	def mergeMessageIdentification(first : Option[CrackerTreeMessageIdentification], second : Option[CrackerTreeMessageIdentification]) : Option[CrackerTreeMessageIdentification] =
	{
		if (first.isDefined) {
			first.get.merge(second)
		} else {
			second
		}
	}

	def mergeMessageTree(first : Option[CrackerTreeMessageTree], second : Option[CrackerTreeMessageTree]) : Option[CrackerTreeMessageTree] =
		{
			if (first.isDefined) {
				first.get.merge(second)
			} else {
				second
			}
		}

	def reduceRed(item1 : CrackerTreeMessageRedPhase, item2 : CrackerTreeMessageRedPhase) : CrackerTreeMessageRedPhase =
		{
			new CrackerTreeMessageRedPhase(mergeMessageIdentification(item1.first, item2.first), mergeMessageTree(item1.second, item2.second))
		}

	def mergeTree(start : Option[RDD[(Long, CrackerTreeMessageTree)]], add : RDD[(Long, CrackerTreeMessageTree)], crackerUseUnionInsteadOfJoin : Boolean, crackerForceEvaluation : Boolean) : Option[RDD[(Long, CrackerTreeMessageTree)]] =
		{
			if (start.isDefined) {
				if(crackerUseUnionInsteadOfJoin)
				{
					Option.apply(start.get.union(add))
				} else
				{
					if(crackerForceEvaluation)
					{
						val treeUpdated = start.get.leftOuterJoin(add).map(t => (t._1, t._2._1.merge(t._2._2).get))
						val forceEvaluation = treeUpdated.count
						Option.apply(treeUpdated)
					} else
					{
						Option.apply(start.get.leftOuterJoin(add).map(t => (t._1, t._2._1.merge(t._2._2).get)))
					}
				}
			} else {
				Option.apply(add)
			}
		}
	
	def mergeTree(spark : SparkContext, start : Option[RDD[(Long, CrackerTreeMessageTree)]], add : Array[(Long, CrackerTreeMessageTree)], crackerUseUnionInsteadOfJoin : Boolean, crackerForceEvaluation : Boolean) : Option[RDD[(Long, CrackerTreeMessageTree)]] =
		{
			if (start.isDefined) {
				if(crackerUseUnionInsteadOfJoin)
				{
					Option.apply(start.get.union(spark.parallelize(add)))
				} else
				{
					if(crackerForceEvaluation)
					{
						val treeUpdated = start.get.leftOuterJoin(spark.parallelize(add)).map(t => (t._1, t._2._1.merge(t._2._2).get))
						val forceEvaluation = treeUpdated.count
						Option.apply(treeUpdated)
					} else
					{
						Option.apply(start.get.leftOuterJoin(spark.parallelize(add)).map(t => (t._1, t._2._1.merge(t._2._2).get)))
					}
				}
			} else {
				Option.apply(spark.parallelize(add))
			}
		}
	
	def mergeTree(start : Option[Array[(Long, CrackerTreeMessageTree)]], add : Array[(Long, CrackerTreeMessageTree)]) : Option[Array[(Long, CrackerTreeMessageTree)]] =
		{
			if (start.isDefined) {
				Option.apply(start.get.union(add))
			} else {
				Option.apply(add)
			}
		}

	def reducePrepareDataForPropagation(a : CrackerTreeMessageTree, b : CrackerTreeMessageTree) : CrackerTreeMessageTree =
		{
			var parent = a.parent
			if (parent.isEmpty) parent = b.parent
            var coreNew = a.core
            if(coreNew == -1) coreNew = b.core

			new CrackerTreeMessageTree(parent, a.child ++ b.child, coreNew)
		}
	
	def getMessageNumberForPropagation(step : Int, vertexNumber : Long) =
	{
		val stepPropagation = (step - 1) / 2
		
		(vertexNumber * stepPropagation) + vertexNumber
	}
	
	def getMessageSizeForPropagation(step : Int, vertexNumber : Long) =
	{
		val stepPropagation = (step - 1) / 2
		
		((vertexNumber * 2) * stepPropagation) - vertexNumber
	}
}