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

package crackerDensity

class CrackerTreeMessageIdentification
		(val selfDegree : Int, val candidate: Long, val candidateDegree : Int, val neigh: Map[Long, Int]) extends CrackerMessageSize
{
	def voteToHalt = neigh.isEmpty
	
	def getMessageSize = neigh.size + 1
	
	def getCandidate = candidate
	
	def neighFilter(coreThreshold : Int) =
	{
//        if(selfDegree < 0) throw new NullPointerException("error: selfDegree not valid")
	    if(selfDegree >= coreThreshold)
	    {
	        neigh
	    } else
	    {
	        neigh.filter(t => t._2 < coreThreshold || t._1 == candidate)
	    }
	}
	
	def merge(other : Option[CrackerTreeMessageIdentification]) : Option[CrackerTreeMessageIdentification] =
	{
		if(other.isDefined)
		{
		    Option.apply(merge(other.get))
		} else
		{
			Option.apply(CrackerTreeMessageIdentification.this)
		}
	}
	
	def merge(other : CrackerTreeMessageIdentification) : CrackerTreeMessageIdentification =
	{
	    var selfDegreeCheck = selfDegree
	    if(selfDegreeCheck < 0) selfDegreeCheck = other.selfDegree
	    
	    if(other.candidateDegree == candidateDegree)
	    {
	        if(other.candidate.compareTo(candidate) < 0)
	        {
	            new CrackerTreeMessageIdentification(selfDegreeCheck, other.candidate, other.candidateDegree, neigh ++ other.neigh)
	        } else
	        {
	            new CrackerTreeMessageIdentification(selfDegreeCheck, candidate, candidateDegree, neigh ++ other.neigh)
	        }
	    } else if(other.candidateDegree.compareTo(candidateDegree) > 0)
	    {
	        new CrackerTreeMessageIdentification(selfDegreeCheck, other.candidate, other.candidateDegree, neigh ++ other.neigh)
	    } else
	    {
	        new CrackerTreeMessageIdentification(selfDegreeCheck, candidate, candidateDegree, neigh ++ other.neigh)
	    }
	}
	
	override def toString = candidate+" d:"+selfDegree.toString()+" "+neigh.toString
}

object CrackerTreeMessageIdentification
{
    def apply(selfDegree : Int, candidate : Long, candidateDegree : Int) = new CrackerTreeMessageIdentification(selfDegree, candidate, candidateDegree, Map(candidate -> candidateDegree))
//	def empty[T <: Comparable[T]]() = new CrackerTreeMessageIdentification[T](Option[T].empty, Set())
}