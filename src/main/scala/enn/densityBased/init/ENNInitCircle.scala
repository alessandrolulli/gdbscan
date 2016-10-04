package enn.densityBased.init

import java.util.Random
import enn.densityBased.ENNConfig

class ENNInitCircle(val configHere : ENNConfig) extends ENNInit(configHere) {

    def generateKId(selfId : Long, vertexNumber : Long) : Iterable[Long] =
        {
            (1 to _config.k) map (t =>
                {
                    (selfId + t) % vertexNumber
                })
        }
}