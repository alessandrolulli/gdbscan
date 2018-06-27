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
import knn.graph.NeighborList;
import knn.graph.impl.Node;
import knn.metric.IMetric;

import java.io.*;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author tibo
 * @param <t>
 */
public abstract class GraphBuilder<TID, t, TN extends INode<TID, t>> implements Cloneable, Serializable {
    protected int k = 10;
    protected IMetric<TID, t, TN> similarity;
    protected int computed_similarities = 0;

    public int getK() {
        return k;
    }

    /**
     * Define k the number of edges per node.
     * Default value is 10
     * @param k
     */
    public void setK(int k) {
        if (k <=0) {
            throw new InvalidParameterException("k must be > 0");
        }
        this.k = k;
    }

    public IMetric<TID, t, TN> getSimilarity() {
        return similarity;
    }

    public void setSimilarity(IMetric<TID, t, TN> similarity) {
        this.similarity = similarity;
    }

    public int getComputedSimilarities() {
        return computed_similarities;
    }

    public Map<TN, NeighborList<TID, t, TN>> computeGraph(List<TN> nodes) {
        if (nodes.isEmpty()) {
            throw new InvalidParameterException("Nodes list is empty");
        }

        if (similarity == null) {
            throw new InvalidParameterException("Similarity is not defined");
        }
        computed_similarities = 0;

        return _computeGraph(nodes);
    }

    /**
     * Build the approximate graph, then use brute-force to build the exact
     * graph and compare the results
     * @param nodes
     */
    public void test(List<TN> nodes) {
        final Map<TN, NeighborList<TID, t, TN>> approximate_graph = this.computeGraph(nodes);

        // Use Brute force to build the exact graph
        final BruteForce brute = new BruteForce();
        brute.setK(k);
        brute.setSimilarity(similarity);
        final Map<TN, NeighborList<TID, t, TN>> exact_graph = brute.computeGraph(nodes);

        int correct = 0;
        for (final TN node : nodes) {
            correct += approximate_graph.get(node).CountCommonValues(exact_graph.get(node));
        }

        System.out.println("Theoretial speedup: " + this.estimatedSpeedup());
        System.out.println("Computed similarities: " + this.getComputedSimilarities());
        final double speedup_ratio =
                (double) ((nodes.size() * (nodes.size() - 1)) / 2) /
                this.getComputedSimilarities();
        System.out.println("Speedup ratio: " + speedup_ratio);

        final double correct_ratio = (double) correct / (nodes.size() * k);
        System.out.println("Correct edges: " + correct +
                "(" + (correct_ratio * 100) + "%)");

        System.out.println("Quality-equivalent speedup: "
                + (speedup_ratio * correct_ratio));
    }

    public double estimatedSpeedup() {
        return 1.0;
    }

    public static List<Node<String>> readFile(String path) {
        try {
            FileReader fileReader;
            fileReader = new FileReader(path);

            final BufferedReader bufferedReader = new BufferedReader(fileReader);
            final List<Node<String>> nodes = new ArrayList<Node<String>>();
            String line = null;
            int i = 0;
            while ((line = bufferedReader.readLine()) != null) {
                nodes.add(new Node(String.valueOf(i), line));
                i++;
            }
            bufferedReader.close();
            return  nodes;
        } catch (final FileNotFoundException ex) {
            Logger.getLogger(GraphBuilder.class.getName()).log(Level.SEVERE, null, ex);
        } catch (final IOException ex) {
            Logger.getLogger(GraphBuilder.class.getName()).log(Level.SEVERE, null, ex);
        }

        return null;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    protected abstract Map<TN, NeighborList<TID, t, TN>> _computeGraph(List<TN> nodes);
}
