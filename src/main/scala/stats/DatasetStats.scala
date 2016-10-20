package stats

import enn.densityBased.ENNConfig
import dataset.DatasetLoad

object DatasetStats {
  def main(args_ : Array[String]): Unit =
    {
      val config = new ENNConfig(args_, "DATASET_STATS")
      val sc = config.util.getSparkContext();

      val file = sc.textFile(config.property.dataset, config.property.sparkPartition)

      config.propertyLoad.get("ennType", "String") match {
        case "BagOfWords" | "BagOfWordsMAP" =>
          {
            val vertexRDD = DatasetLoad.loadBagOfWords(file, config.property, config)

            val n = vertexRDD.count()
            val sizeRDD = vertexRDD.map(t => t._2.size())
            val d = sizeRDD.max()
            val avg = sizeRDD.mean()
            val freq = sizeRDD.map(t => (t, 1)).reduceByKey(_+_)
            
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
      }
    }
}