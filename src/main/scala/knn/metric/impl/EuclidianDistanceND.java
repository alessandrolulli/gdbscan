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
import knn.graph.NeighborListComparatorASC;
import knn.graph.NeighborListFactory;
import knn.metric.IMetric;
import knn.util.PointND;

public class EuclidianDistanceND<TID, T extends PointND, TN extends INode<TID, T>> implements IMetric<TID, T, TN>
{
	private static final long serialVersionUID = 1L;

	@Override
	public double compare(T a_, T b_)
	{
		double sum = 0;
		for(int i = 0 ; i < a_.size() ; i++)
		{
			sum += Math.pow(a_.get(i) - b_.get(i), 2);
		}
		
		return Math.sqrt(sum);
	}

	@Override
	public boolean isValid(Neighbor<TID, T, TN> neighbor_, double epsilon_)
	{
		return isValid(neighbor_.similarity, epsilon_);
	}
	
	public boolean isValid(double val_, double epsilon_) 
	{
		return val_ <= epsilon_;
	}

	@Override
	public NeighborListFactory<TID, T, TN> getNeighborListFactory()
	{
		return new NeighborListFactory<>(new NeighborListComparatorASC<TID, T, TN>());
	}

	@Override
	public double similarity(T a_, T b_)
	{
		return 1 - compare(a_, b_);
	}
}
