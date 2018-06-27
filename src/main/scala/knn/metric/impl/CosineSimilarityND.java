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
import knn.graph.NeighborListComparatorDESC;
import knn.graph.NeighborListFactory;
import knn.metric.IMetric;
import knn.util.IPointND;

public class CosineSimilarityND<TID, T extends IPointND, TN extends INode<TID, T>> implements IMetric<TID, T, TN>
{
	private static final long serialVersionUID = 1L;

//	public static void main(String[] args_)
//	{
//		final CosineSimilarityND sim = new CosineSimilarityND();
//		final PointND a = new PointND(new double[]{1,2,1,1,3});
//		final PointND b = new PointND(new double[]{1,0,0,0,5});
//
//		System.out.println(sim.compare(a, b));
//	}

	@Override
	public double compare(T a_, T b_)
	{
		double dotProduct = 0.0;
	    double normA = 0.0;
	    double normB = 0.0;
	    for (int i = 0; i < a_.size(); i++)
	    {
	        dotProduct += a_.get(i) * b_.get(i);
	        normA += Math.pow(a_.get(i), 2);
	        normB += Math.pow(b_.get(i), 2);
	    }
	    return (dotProduct / (Math.sqrt(normA) * Math.sqrt(normB)));
	}

	@Override
	public boolean isValid(Neighbor<TID, T, TN> neighbor_, double epsilon_)
	{
		return neighbor_.similarity >= epsilon_;
	}

	@Override
	public NeighborListFactory<TID, T, TN> getNeighborListFactory()
	{
		return new NeighborListFactory<>(new NeighborListComparatorDESC<TID, T, TN>());
	}

	@Override
	public double similarity(T a_, T b_)
	{
		return compare(a_, b_);
	}
}

