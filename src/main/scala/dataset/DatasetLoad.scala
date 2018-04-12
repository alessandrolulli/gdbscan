package dataset

import java.util.HashSet

import enn.densityBased.ENNConfig
import knn.util.{PointND, PointNDBoolean, PointNDSparse}
import org.apache.spark.rdd.RDD
import util.CCPropertiesImmutable

import scala.collection.JavaConversions._

object DatasetLoad {
  def loadBagOfWords( data : RDD[String], property : CCPropertiesImmutable , config : ENNConfig) : RDD[( Long, PointNDSparse )] =
    {
        val toReturnEdgeList : RDD[( Long, (Int, Int))] = data.flatMap( line =>
            {
                val splitted = line.split( " " )
                if ( splitted.size >= 3 ) {
                    try {
                        Some( splitted( 0 ).toLong, (splitted(1).toInt, splitted(2).toInt) )
                    } catch {
                        case e : Exception => None
                    }
                } else {
                    None
                }
            } )
            
        val toReturn = toReturnEdgeList.groupByKey.map(t => 
          {
            val size = t._2.size
            val sorted = t._2.toList.sortWith(_._1 < _._1)
            val point = new PointNDSparse(size)
            
            sorted.zipWithIndex.foreach(u => point.add(u._2, u._1._1, u._1._2))
            
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
  
  def loadTransactionData( data : RDD[String], property : CCPropertiesImmutable ) : RDD[( Long, java.util.Set[Int] )] =
    {
        val toReturnEdgeList : RDD[( Long, java.util.Set[Int] )] = data.map( line =>
            {
                val splitted = line.split( ";" )
//                val splitted = line.split( property.separator )
                if ( splitted.size >= 2 ) {
                    try {
                        val set : java.util.Set[Int] = new java.util.HashSet
                        val elSet = splitted(2).split(" ").map(_.toInt).toSet
                        set.addAll(elSet)
                        ( splitted( 0 ).toLong, set )
                    } catch {
                        case e : Exception => ( -1L, new HashSet() )
                    }
                } else {
                    ( -1L, new HashSet() )
                }
            } )

        toReturnEdgeList.filter( t => !t._2.isEmpty )
    }
  
  def loadHousehold( data : RDD[String], property : CCPropertiesImmutable ) : RDD[( Long, PointND )] =
    {
        val toReturnEdgeList : RDD[( Long, PointND )] = data.map( line =>
            {
                val splitted = line.split( ";" )
//                val splitted = line.split( property.separator )
                if ( splitted.size >= 2 ) {
                    try {
                        ( splitted( 0 ).toLong, new PointND(splitted.slice(3, 10).map(t => t.toDouble).toArray) )
                    } catch {
                        case e : Exception => ( -1L, PointND.NOT_VALID )
                    }
                } else {
                    ( -1L, PointND.NOT_VALID )
                }
            } )

        toReturnEdgeList.filter( t => t._2.size() > 0 )
    }
  
  def loadStringData(data : RDD[String], property : CCPropertiesImmutable, config : ENNConfig ) : RDD[(String, String)] =
    {
        val toReturnEdgeList : RDD[(String, String)] = data.map(line =>
            {
                val splitted = line.split(property.separator)
                if (/*splitted.size >= 1 &&*/ !splitted(0).trim.isEmpty) {
                    try {
                        (splitted( 0 ), splitted( config.columnDataA ))
                    } catch {
                        case e : Exception => ("EMPTY","EMPTY")
                    }
                } else {
                    ("EMPTY","EMPTY")
                }
            })

        toReturnEdgeList.filter(t => !t._1.equals("EMPTY"))
    }

  def loadPointND( data : RDD[String], property : CCPropertiesImmutable, dimensionLimit : Int, config : ENNConfig ) : RDD[( String, PointND )] =
  {
    val toReturnEdgeList : RDD[( String, PointND )] = data.map( line =>
    {
      val splitted = line.split( property.separator )
      if (!splitted( 0 ).trim.isEmpty ) {
        try {
          ( splitted( 0 ), new PointND(splitted(config.columnDataA).split(" ").slice(0, dimensionLimit).map(x => x.toDouble) ))
        } catch {
          case e : Exception => ( "EMPTY", PointND.NOT_VALID )
        }
      } else {
        ( "EMPTY", PointND.NOT_VALID )
      }
    } )

    toReturnEdgeList.filter( t => t._2.size() > 0 )
  }

  def loadCluster(data: RDD[String], split : String = "\t"): RDD[(Long, Long)] = {
    val toReturn : RDD[(Long, Long)] = data.map(line => {
      val splitted = line.split(split)
      if (splitted.size > 1) {
        try {
        (splitted(0).toLong, splitted(1).toLong)
        } catch {
          case e : Exception => (-1, -1)
        }
      } else {
        (-1, -1)
      }
    })

    toReturn.filter(t => t._1 != -1)
  }
}