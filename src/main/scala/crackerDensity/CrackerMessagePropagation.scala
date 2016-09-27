package crackerDensity

class CrackerTreeMessagePropagation(val min : Option[Long], val child : Set[Long], val core : Integer) extends CrackerMessageSize
{
	def getMessageSize = child.size + 1
}