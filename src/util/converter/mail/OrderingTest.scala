package util.converter.mail

import scala.util.Sorting

class OrderingTest extends Ordering[(String, Int, Set[String])] 
{
	 def compare(a:(String, Int, Set[String]), b:(String, Int, Set[String])) = 
	 {
	     b._2 compare a._2
	 }
}