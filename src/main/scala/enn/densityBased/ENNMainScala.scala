/*
 * Copyright (C) 2011-2012 the original author or authors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package enn.densityBased

object ENNMainScala {
  def main(args_ : Array[String]): Unit = {
    val timeBegin = System.currentTimeMillis

    val ennLoader = new ENNLoader(args_)

    if (!ennLoader.config.skipENN) {
      ennLoader.loadAndStart

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

    if (!ennLoader.config.skipCluster) {
      crackerDensity.CrackerAllComparable.mainGO(ennLoader.config.property.outputFile, args_, ennLoader.sc)
    }

    if (!ennLoader.config.skipInternalEvaluation) {
      stats.ClusteringInternalEvaluation.main(args_, ennLoader.sc)
    }

    if (!ennLoader.config.skipExternalEvaluation) {
      stats.ClusteringExternalEvaluation.main(args_, ennLoader.sc)
    }

    ennLoader.stop
  }
}