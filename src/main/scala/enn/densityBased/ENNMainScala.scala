package enn.densityBased

import org.apache.spark.api.java.JavaRDD.toRDD

import util.CCProperties
import util.CCUtil

object ENNMainScala {
    def main(args_ : Array[String]) : Unit =
        {
            val timeBegin = System.currentTimeMillis

            val ennLoader = new ENNLoader(args_)

            if(!ennLoader.config.skipENN)
            {
              val result = ennLoader.loadAndStart
              
              val timeEnd = System.currentTimeMillis
              
              ennLoader.config.util.io.printStatENN(
                ennLoader.config.endIterationValue,
                timeEnd - timeBegin,
                ennLoader.config.k,
                ennLoader.config.kMax,
                ennLoader.config.epsilon,
                ennLoader.config.randomRestart,
                ennLoader.config.printStep,
                0,
                0,
                0,
                ennLoader.config.performance,
                0,
                ennLoader.config)
            }
            
            ennLoader.stop

            if(!ennLoader.config.skipCluster)
            {
              crackerDensity.CrackerAllComparable.mainGO(ennLoader.config.property.outputFile, args_)
            }
        }
}