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

package knn.graph.generation;

import knn.graph.INode;
import knn.graph.Neighbor;
import knn.graph.NeighborList;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;

/**
 *
 * @author Thibault Debatty
 */
public class BruteForce<TID, t, TN extends INode<TID, t>> extends GraphBuilder<TID, t, TN> {

	private static final long serialVersionUID = 1L;

	@Override
    public HashMap<TN, NeighborList<TID, t, TN>> _computeGraph(List<TN> nodes) {

        final int n = nodes.size();

        // Initialize all NeighborLists
        final HashMap<TN, NeighborList<TID, t, TN>> neighborlists = new HashMap<TN, NeighborList<TID, t, TN>>(n);
        for (final TN node : nodes) {
            neighborlists.put(node, new NeighborList<TID, t, TN>(k));
        }

        computed_similarities = 0;
        double sim;
        TN n1;
        TN n2;
        final HashMap<String, Object> data = new HashMap<String, Object>();

        for (int i = 0; i < n; i++) {

            n1 = nodes.get(i);
            for (int j = 0; j < i; j++) {
                n2 = nodes.get(j);
                sim = similarity.similarity(n1.getValue(), n2.getValue());
                computed_similarities++;

                neighborlists.get(n1).add(new Neighbor<TID, t, TN>(n2, sim));
                neighborlists.get(n2).add(new Neighbor<TID, t, TN>(n1, sim));
            }
        }

        return neighborlists;
    }

    public Tuple2<HashMap<TN, NeighborList<TID, t, TN>>, Double> computeGraphAndAvgSimilarity(List<TN> nodes) {

        final int n = nodes.size();

        // Initialize all NeighborLists
        final HashMap<TN, NeighborList<TID, t, TN>> neighborlists = new HashMap<TN, NeighborList<TID, t, TN>>(n);
        for (final TN node : nodes) {
            neighborlists.put(node, new NeighborList<TID, t, TN>(k));
        }

        computed_similarities = 0;
        double sim;
        TN n1;
        TN n2;
        final HashMap<String, Object> data = new HashMap<String, Object>();

        double sum = 0;
        int count = 0;

        final DescriptiveStatistics stat = new DescriptiveStatistics();

        for (int i = 0; i < n; i++) {

            n1 = nodes.get(i);
            for (int j = 0; j < i; j++) {
                n2 = nodes.get(j);
                sim = similarity.similarity(n1.getValue(), n2.getValue());
                computed_similarities++;

                count++;
                sum += sim;
                stat.addValue(sim);

                neighborlists.get(n1).add(new Neighbor<TID, t, TN>(n2, sim));
                neighborlists.get(n2).add(new Neighbor<TID, t, TN>(n1, sim));
            }
        }

//        return new Tuple2(neighborlists, stat.getPercentile(90));
        return new Tuple2(neighborlists, stat.getMean());
    }
}