package enn.densityBased

import knn.util.PointNDSparse
import knn.metric.impl.CosineSimilarityNDSparse

object prova {
  val a = List("3430"
,"6906"
,"353160","1 61 2",
"1 76 1",
"1 89 1",
"1 211 1",
"1 296 1",
"1 335 1",
"1 404 1",
"2 4579 1",
"2 4599 1",
"2 4604 1",
"2 4639 3",
"2 5190 1",
"2 5229 1",
"2 5524 2",
"2 5582 1",
"2 5810 2",
"2 5843 1",
"3 869 1",
"3 938 1",
"3 987 1",
"3 989 1")                                        //> a  : List[String] = List(3430, 6906, 353160, 1 61 2, 1 76 1, 1 89 1, 1 211 1
                                                  //| , 1 296 1, 1 335 1, 1 404 1, 2 4579 1, 2 4599 1, 2 4604 1, 2 4639 3, 2 5190 
                                                  //| 1, 2 5229 1, 2 5524 2, 2 5582 1, 2 5810 2, 2 5843 1, 3 869 1, 3 938 1, 3 987
                                                  //|  1, 3 989 1)

 val toReturnEdgeList = a.map( line =>
            {
                val splitted = line.split( " " )
                if ( splitted.size >= 3 ) {
                    try {
                        ( splitted( 0 ).toLong, (splitted(1).toInt, splitted(2).toInt) )
                    } catch {
                        case e : Exception => ( -1L, (-1,-1) )
                    }
                } else {
                    ( -1L, (-1,-1) )
                }
            } ).filter( t => t._1 > 0 )           //> toReturnEdgeList  : List[(Long, (Int, Int))] = List((1,(61,2)), (1,(76,1)), 
                                                  //| (1,(89,1)), (1,(211,1)), (1,(296,1)), (1,(335,1)), (1,(404,1)), (2,(4579,1))
                                                  //| , (2,(4599,1)), (2,(4604,1)), (2,(4639,3)), (2,(5190,1)), (2,(5229,1)), (2,(
                                                  //| 5524,2)), (2,(5582,1)), (2,(5810,2)), (2,(5843,1)), (3,(869,1)), (3,(938,1))
                                                  //| , (3,(987,1)), (3,(989,1)))
   
   
   
   val k = toReturnEdgeList.groupBy(_._1).map(t =>
          {
            val size = t._2.size
            val sorted = t._2.toList.sortWith(_._2._1 < _._2._1)
            val point = new PointNDSparse(size)
            
            sorted.zipWithIndex.map(u => point.add(u._2, u._1._2._1, u._1._2._2))
           
            (t._1, point)
          })                                      //> k  : scala.collection.immutable.Map[Long,knn.util.PointNDSparse] = Map(2 ->
                                                  //|  4579 1|||4599 1|||4604 1|||4639 3|||5190 1|||5229 1|||5524 2|||5582 1|||58
                                                  //| 10 2|||5843 1|||, 1 -> 61 2|||76 1|||89 1|||211 1|||296 1|||335 1|||404 1||
                                                  //| |, 3 -> 869 1|||938 1|||987 1|||989 1|||)

}