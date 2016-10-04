package enn.densityBased.init

import enn.densityBased.ENNConfig
import java.util.Random

abstract class ENNInit(val _config : ENNConfig) extends Serializable
{
    def generateKId(selfId : Long, vertexNumber : Long) : Iterable[Long]
    
    def nextLong(rng : Random, n : Long) : Long = {
        // error checking and 2^x checking removed for simplicity.
        var bits : Long = 0L
        var value : Long = 0L
        do {
            bits = (rng.nextLong() << 1) >>> 1;
            value = bits % n;
        } while (bits - value + (n - 1) < 0L);

        if (value >= n) nextLong(rng, n)
        else value;
    }
}