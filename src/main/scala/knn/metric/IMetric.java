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

package knn.metric;

import knn.graph.INode;
import knn.graph.Neighbor;
import knn.graph.NeighborListFactory;

import java.io.Serializable;

public interface IMetric<TID, T, TN extends INode<TID, T>> extends Serializable
{
	/*
	 * this will calculate the similarity or the distance depending on what is necessary to make comparisons
	 */
	 public double compare(T a_, T b_);

	 public boolean isValid(Neighbor<TID, T, TN> neighbor_, double epsilon_);

	 public NeighborListFactory<TID, T, TN> getNeighborListFactory();
	 
	 public double similarity(T a_, T b_);
}
