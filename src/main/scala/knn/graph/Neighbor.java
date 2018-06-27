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

package knn.graph;

import java.io.Serializable;
import java.security.InvalidParameterException;

/**
 *
 * @author Thibault Debatty
 */
public class Neighbor<TID, T, TN extends INode<TID, T>> implements Comparable<T>, Serializable {
    
	private static final long serialVersionUID = 1L;
	public TN node;
    public double similarity;

    public Neighbor(TN node, double similarity) {
        this.node = node;
        this.similarity = similarity;
    }

    /**
     *
     * @return (node.id,node.value,similarity)
     */
    @Override
    public String toString() {
        return "(" + node.getId() + "," + node.getValue() + "," + similarity + ")";
    }


    @Override
    public boolean equals(Object other) {
        if(!(other instanceof Neighbor)) return false;

		final Neighbor<TID,T,TN> other_neighbor = (Neighbor<TID,T,TN>) other;
        return this.node.equals(other_neighbor.node);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = (17 * hash) + (this.node != null ? this.node.hashCode() : 0);
        return hash;
    }

    @Override
    public int compareTo(Object other) {
        if(!(other instanceof Neighbor)) throw new InvalidParameterException();

        if (((Neighbor) other).node.equals(this.node)) {
            return 0;
        }

        if (this.similarity == ((Neighbor)other).similarity) {
            return 0;
        }

        return this.similarity > ((Neighbor)other).similarity ? 1 : -1;
    }
}
