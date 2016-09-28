package enn.densityBased

import scala.collection.JavaConversions._

import util.CCProperties
import util.CCPropertiesImmutable
import util.CCUtil

/**
 * @author alemare
 */
class ENNConfig( args_ : Array[String], appName : String = "ENN" ) extends Serializable
{
    val propertyLoad = ( new CCProperties( appName, args_( 0 ) ) ).load();
    val property = propertyLoad.getImmutable;

    val k = propertyLoad.getInt( "k", 5 );
    val kMax = propertyLoad.getInt( "kMax", 20 );
    val epsilon = propertyLoad.getDouble( "epsilon", 0.9 );
    val alpha = propertyLoad.getDouble( "alpha", 0.4 );
    val addNeighborProbability = propertyLoad.getDouble( "addNeighborProbability", 1.0 );
    val maxIterations = propertyLoad.getInt( "maxIterations", 5 );
    val maxComputingNodes = propertyLoad.getInt( "maxComputingNodes", 500000 );
    val randomRestart = propertyLoad.getInt( "randomRestart", 5 );
    val printStep = propertyLoad.getInt( "printStep", 1 );

    val knnSkip = propertyLoad.getBoolean( "knnSkip", false );
    val clusterSkip = propertyLoad.getBoolean( "clusterSkip", false );
    val clusterMinSize = propertyLoad.getInt( "clusterMinSize", 100 );
    val printOutput = propertyLoad.getBoolean( "printOutput", true );

    val terminationActiveNodes = propertyLoad.getDouble( "terminationActiveNodes", 0.1 );
    val terminationRemovedNodes = propertyLoad.getDouble( "terminationRemovedNodes", 0.0001 );
    val sampling = propertyLoad.getInt( "sampling", -1 );

    val columnDataA = propertyLoad.getInt( "columnDataA", 2 );
    val columnDataB = propertyLoad.getInt( "columnDataB", 3 );

    val neigbourAggregation = propertyLoad.get( "neigbourAggregation", "normal" );
    val outputFileCluster = propertyLoad.get( "outputFileCluster", "" );
    val excludeNode = propertyLoad.getBoolean( "excludeNode", false );
    val filterSparse = propertyLoad.getBoolean( "filterSparse", false );

    val performance = propertyLoad.getBoolean( "performance", true );
    val messageStat = propertyLoad.getBoolean( "messageStat", false );
    val endIterationValue = if (performance) -1 else -2
    val knnMetricDoubleCalculation = propertyLoad.getBoolean( "knnMetricDoubleCalculation", true );

    val epsilonStart = propertyLoad.getInt( "epsilonStart", 0 );
    val epsilonEnd = propertyLoad.getInt( "epsilonEnd", 10 );
    val epsilonIncrement = propertyLoad.getInt( "epsilonIncrement", 1 );

    val dimensionLimit = propertyLoad.getInt( "dimensionLimit", 2 );

    val util = new CCUtil( property );

}