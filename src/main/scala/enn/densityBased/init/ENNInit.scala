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

package enn.densityBased.init

import java.util.Random

import enn.densityBased.ENNConfig

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