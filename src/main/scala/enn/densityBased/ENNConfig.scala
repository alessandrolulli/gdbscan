package enn.densityBased

import scala.collection.JavaConversions._

import util.CCProperties
import util.CCPropertiesImmutable
import util.CCUtil
import enn.densityBased.init.ENNInitRandom
import enn.densityBased.init.ENNInitCircle
import enn.densityBased.init.ENNInitSystematicSampling

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

    val (groundtruth, groundtruthAvailable) : (String, Boolean) = propertyLoad.get("groundtruth", "NO") match
    {
        case "NO" => ("NO", false)
        case "SAME" => (property.dataset, true)
        case default => (default, true)
    }

    val ennSkip = propertyLoad.getBoolean( "ennSkip", false );
    val knnSkip = propertyLoad.getBoolean( "knnSkip", false );
    val clusterSkip = propertyLoad.getBoolean( "clusterSkip", false );
    val internalEvaluationSkip = propertyLoad.getBoolean( "internalEvaluationSkip", true );
    val externalEvaluationSkip = propertyLoad.getBoolean( "externalEvaluationSkip", false ) || !groundtruthAvailable;
    val skipENN = ennSkip;
    val skipCluster = clusterSkip;
    val skipInternalEvaluation = internalEvaluationSkip;
    val skipExternalEvaluation = externalEvaluationSkip;
    
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
    val knnMetricDoubleCalculation = propertyLoad.getBoolean( "knnMetricDoubleCalculation", false );

    val epsilonStart = propertyLoad.getInt( "epsilonStart", 0 );
    val epsilonEnd = propertyLoad.getInt( "epsilonEnd", 10 );
    val epsilonIncrement = propertyLoad.getInt( "epsilonIncrement", 1 );
    val instrumented = propertyLoad.getBoolean( "instrumented", false );

    val dimensionLimit = propertyLoad.getInt( "dimensionLimit", 2 );
    
    val initPolicy = propertyLoad.get("initPolicy", "RANDOM") match
		{
			case "RANDOM" => new ENNInitRandom(this)
			case "CIRCLE" => new ENNInitCircle(this)
			case "SYSTEMATIC" => new ENNInitSystematicSampling(this)
			case _ => new ENNInitRandom(this)
		}

    val util = new CCUtil( property );

}