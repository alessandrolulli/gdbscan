package enn.densityBased.init

import java.util.Random
import enn.densityBased.ENNConfig

class ENNInitSystematicSampling(val configHere : ENNConfig) extends ENNInit(configHere) {

    def generateKId(selfId : Long, vertexNumber : Long) : Iterable[Long] =
        {
            val rand = new Random

            val interval = vertexNumber + selfId
            val startingPoint = nextLong(rand, interval)

            (1 to _config.k) map (t =>
                {
                    (startingPoint + (t * interval)) % vertexNumber
                })
        }
}