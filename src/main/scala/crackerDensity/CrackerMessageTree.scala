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

class CrackerTreeMessageTree(val parent : Option[Long], val child : Set[Long], val core : Int) extends CrackerMessageSize
{
	def getMessageSize = child.size + 1
	
	def merge(other : Option[CrackerTreeMessageTree]) : Option[CrackerTreeMessageTree] =
	{
		if(other.isDefined)
		{
			var parentNew = parent
            var coreNew = core
			
            if(coreNew == -1)
            {
                coreNew = other.get.core
            }
			if(parentNew.isEmpty)
			{
				parentNew = other.get.parent
			}
			
			Option.apply(new CrackerTreeMessageTree(parentNew, child ++ other.get.child, coreNew))
		} else
		{
			Option.apply(CrackerTreeMessageTree.this)
		}
	}
	
	def merge(other : CrackerTreeMessageTree) : CrackerTreeMessageTree =
	{
		var parentNew = parent
        var coreNew = core
		
		if(parentNew.isEmpty)
		{
			parentNew = other.parent
		}
        if(coreNew == -1)
        {
            coreNew = other.core
        }
		
		new CrackerTreeMessageTree(parentNew, child ++ other.child, coreNew)
	}
	
	def getMessagePropagation(id : Long) = 
	{
		if(parent.isEmpty)
		{
            if(core > 0)
			    new CrackerTreeMessagePropagation(Option.apply(id), child, core)
            else
                new CrackerTreeMessagePropagation(Option.apply(-1), child, core)
		} else
		{
			new CrackerTreeMessagePropagation(Option.empty, child, core)
		}
	}
}

object CrackerTreeMessageTree
{
	def empty = new CrackerTreeMessageTree(Option.empty, Set(), -1)
}