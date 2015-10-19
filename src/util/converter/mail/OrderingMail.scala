package util.converter.mail

import scala.util.Sorting

class OrderingMail extends Ordering[(String, Int)] 
{
	 def compare(a:(String, Int), b:(String, Int)) = 
	 {
	     b._2 compare a._2
	 }
}

object OrderingMail
{
	def apply = new OrderingMail
}