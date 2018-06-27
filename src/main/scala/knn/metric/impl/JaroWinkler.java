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

package knn.metric.impl;

import knn.graph.INode;
import knn.graph.Neighbor;
import knn.graph.NeighborListFactory;
import knn.metric.IMetric;

import java.util.Arrays;

/**
 *
 * @author tibo
 */
public class JaroWinkler<TID, TN extends INode<TID, String>> implements IMetric<TID, String, TN>
{
	private static final long serialVersionUID = 1L;

    public JaroWinkler() {

    }

    public JaroWinkler(double threshold) {
        this.setThreshold(threshold);
    }

    private double threshold = 0.7;

    /**
     * Sets the threshold used to determine when Winkler bonus should be used.
     * Set to a negative value to get the Jaro distance.
     * Default value is 0.7
     *
     * @param threshold the new value of the threshold
     */
    public final void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    /**
     * Returns the current value of the threshold used for adding the Winkler
     * bonus. The default value is 0.7.
     *
     * @return the current value of the threshold
     */
    public double getThreshold() {
        return threshold;
    }

    @Override
	public double similarity(String s1, String s2) {
        final int[] mtp = matches(s1, s2);
        final float m = mtp[0];
        if (m == 0) {
            return 0f;
        }
        final float j = (((m / s1.length()) + (m / s2.length()) + ((m - mtp[1]) / m))) / 3;
        final float jw = j < getThreshold() ? j : j + (Math.min(0.1f, 1f / mtp[3]) * mtp[2]
                * (1 - j));
        return jw;
    }


	public double distance(String s1, String s2) {
        return 1.0 - similarity(s1, s2);
    }


    private int[] matches(String s1, String s2) {
        String max, min;
        if (s1.length() > s2.length()) {
            max = s1;
            min = s2;
        } else {
            max = s2;
            min = s1;
        }
        final int range = Math.max((max.length() / 2) - 1, 0);
        final int[] matchIndexes = new int[min.length()];
        Arrays.fill(matchIndexes, -1);
        final boolean[] matchFlags = new boolean[max.length()];
        int matches = 0;
        for (int mi = 0; mi < min.length(); mi++) {
            final char c1 = min.charAt(mi);
            for (int xi = Math.max(mi - range, 0),
                    xn = Math.min(mi + range + 1, max.length()); xi < xn; xi++) {
                if (!matchFlags[xi] && (c1 == max.charAt(xi))) {
                    matchIndexes[mi] = xi;
                    matchFlags[xi] = true;
                    matches++;
                    break;
                }
            }
        }
        final char[] ms1 = new char[matches];
        final char[] ms2 = new char[matches];
        for (int i = 0, si = 0; i < min.length(); i++) {
            if (matchIndexes[i] != -1) {
                ms1[si] = min.charAt(i);
                si++;
            }
        }
        for (int i = 0, si = 0; i < max.length(); i++) {
            if (matchFlags[i]) {
                ms2[si] = max.charAt(i);
                si++;
            }
        }
        int transpositions = 0;
        for (int mi = 0; mi < ms1.length; mi++) {
            if (ms1[mi] != ms2[mi]) {
                transpositions++;
            }
        }
        int prefix = 0;
        for (int mi = 0; mi < min.length(); mi++) {
            if (s1.charAt(mi) == s2.charAt(mi)) {
                prefix++;
            } else {
                break;
            }
        }
        return new int[]{matches, transpositions / 2, prefix, max.length()};
    }

	@Override
	public double compare(String a_, String b_) {
		return similarity(a_, b_);
	}

	@Override
	public boolean isValid(Neighbor<TID, String, TN> neighbor_, double epsilon_) {
		return neighbor_.similarity > epsilon_;
	}

	@Override
	public NeighborListFactory<TID, String, TN> getNeighborListFactory()
	{
		return new NeighborListFactory<TID, String, TN>();
	}
}
