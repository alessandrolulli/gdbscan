package enn.densityBased.init

import java.util.Random
import enn.densityBased.ENNConfig

class ENNInitRandom(val configHere : ENNConfig) extends ENNInit(configHere) {

    def generateKId(selfId : Long, vertexNumber : Long) : Iterable[Long] =
        {
            val rand = new Random

            (1 to _config.k) map (_ =>
                {
                    nextLong(rand, vertexNumber)
                })
        }
}