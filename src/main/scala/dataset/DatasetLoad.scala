package dataset

import util.CCPropertiesImmutable
import org.apache.spark.rdd.RDD
import enn.densityBased.ENNConfig
import knn.util.PointNDSparse
import knn.util.PointNDBoolean

object DatasetLoad {
  def loadBagOfWords( data : RDD[String], property : CCPropertiesImmutable , config : ENNConfig) : RDD[( Long, PointNDSparse )] =
    {
        val toReturnEdgeList : RDD[( Long, (Int, Int))] = data.map( line =>
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
            } ).filter( t => t._1 > 0 )
            
        val toReturn = toReturnEdgeList.groupByKey.map(t => 
          {
            val size = t._2.size
            val sorted = t._2.toList.sortWith(_._1 < _._1)
            val point = new PointNDSparse(size)
            
            sorted.zipWithIndex.map(u => point.add(u._2, u._1._1, u._1._2))
            
            (t._1, point)
          })
          
        toReturn
    }
  
  def loadImageBinary( data : RDD[String], property : CCPropertiesImmutable , config : ENNConfig) : RDD[( Long, PointNDBoolean )] =
    {
        val toReturnEdgeList : RDD[( Long, PointNDBoolean)] = data.map( line =>
            {
                val splitted = line.split( "," )
                if ( splitted.size >= 258 ) {
                    try {
                      val boolArray = splitted.slice(2, 259).map(t => {if (t == "1") true else false})
                      ( splitted( 0 ).toLong, new PointNDBoolean(boolArray) )
                    } catch {
                        case e : Exception => {
                          ( -1L, PointNDBoolean.NOT_VALID )
                        }
                    }
                } else {
                    ( -1L, PointNDBoolean.NOT_VALID )
                }
            } ).filter( t => t._1 > 0 )
            
        toReturnEdgeList
    }
}