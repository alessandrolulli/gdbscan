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

package stats

import dataset.DatasetLoad
import enn.densityBased.ENNConfig
import org.apache.spark.rdd.RDD

object DatasetStats {

  def printOutput(rdd: RDD[Int], config: ENNConfig) =
    {
      val n = rdd.count()

      val d = rdd.max()
      val avg = rdd.mean()
      val freq = rdd.map(t => (t, 1)).reduceByKey(_ + _)

      config.util.io.printData("stats_dataset.txt",
        config.property.dataset,
        n.toString,
        d.toString,
        avg.toString)

      freq.collect.foreach(t => config.util.io.printData("stats_dataset_freq.txt",
        config.property.dataset,
        t._1.toString,
        t._2.toString))
    }

  def main(args_ : Array[String]): Unit =
    {
      val config = new ENNConfig(args_, "DATASET_STATS")
      val sc = config.util.getSparkContext();

      val file = sc.textFile(config.property.dataset, config.property.sparkPartition)

      config.propertyLoad.get("ennType", "String") match {
        case "BagOfWords" | "BagOfWordsMAP" =>
          {
            val vertexRDD = DatasetLoad.loadBagOfWords(file, config.property, config)
            val sizeRDD = vertexRDD.map(t => t._2.size())

            printOutput(sizeRDD, config)
          }
        case "Transaction" | "TransactionMAP" =>
          {
            val vertexRDD = DatasetLoad.loadTransactionData(file, config.property)
            val sizeRDD = vertexRDD.map(t => t._2.size())

            printOutput(sizeRDD, config)
          }
      }
    }
}