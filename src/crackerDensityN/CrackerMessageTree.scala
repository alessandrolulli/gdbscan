package crackerDensityN

@serializable
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