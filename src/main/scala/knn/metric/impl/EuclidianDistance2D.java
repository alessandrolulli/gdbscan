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
import knn.util.Point2D;

public class EuclidianDistance2D<TID, TN extends INode<TID, Point2D>> implements IMetric<TID, Point2D, TN>
{
	private static final long serialVersionUID = 1L;

	@Override
	public double compare(Point2D a_, Point2D b_)
	{
		return Math.sqrt(Math.pow(a_.getX() - b_.getX(), 2) + Math.pow(a_.getY() - b_.getY(), 2));
	}

	@Override
	public boolean isValid(Neighbor<TID, Point2D, TN> neighbor_, double epsilon_)
	{
		return neighbor_.similarity <= epsilon_;
	}

	@Override
	public NeighborListFactory<TID, Point2D, TN> getNeighborListFactory()
	{
		return new NeighborListFactory<>(new NeighborListComparatorASC<TID, Point2D, TN>());
	}

	@Override
	public double similarity(Point2D a_, Point2D b_)
	{
		return 1 / (1 + compare(a_, b_));
	}

}
