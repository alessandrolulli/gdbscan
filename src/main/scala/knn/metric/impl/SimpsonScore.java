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
import knn.graph.impl.NodeSimple;
import knn.metric.IMetric;
import knn.util.PointNDBoolean;

public class SimpsonScore<TID, TN extends INode<TID, PointNDBoolean>> implements IMetric<TID, PointNDBoolean, TN>
{
	private static final long serialVersionUID = 1L;

	public static void main(String[] args_)
	{
		int[] bInt = new int[]{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,1,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0};
		int[] aInt = new int[]{0,0,0,0,0,0,0,1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,1,1,1,1,1,0,0,0,0,0,0,0,0,0,0,1,1,1,1,1,1,0,0,0,0,0,0,0,0,0,0,1,1,1,0,1,1,1,0,0,0,0,0,0,0,0,1,1,1,0,0,0,1,1,0,0,0,0,0,0,0,1,1,1,0,0,0,0,1,1,0,0,0,0,0,0,0,1,1,1,0,0,0,0,1,1,1,0,0,0,0,0,0,1,1,1,0,0,0,0,0,1,1,0,0,0,0,0,0,1,1,1,0,0,0,0,0,1,1,0,0,0,0,0,1,1,1,1,0,0,0,0,0,1,1,0,0,0,0,0,1,1,1,1,0,0,0,0,0,1,1,1,0,0,0,0,0,1,1,1,0,0,0,0,1,1,1,0,0,0,0,0,0,1,1,1,0,0,1,1,1,1,1,0,0,0,0,0,0,0,1,1,1,1,1,1,1,1,1,0,0,0,0,0,0,0,0,1,1,1,1,1,1,1,0,0,0,0,0,0,0,0,0,0,0,1,1,1,0,0,0,0,0,0};
		
		boolean[] aBool = new boolean[256];
		boolean[] bBool = new boolean[256];
		for(int i = 0 ; i < aBool.length ; i++)
		{
			aBool[i] = aInt[i] == 0 ? false : true;
			bBool[i] = bInt[i] == 0 ? false : true;
		}
		
		final SimpsonScore sim = new SimpsonScore();
		final PointNDBoolean a = new PointNDBoolean(aBool);
		final PointNDBoolean b = new PointNDBoolean(bBool);
		
		double val = sim.compare(a, b);
		double score = 2-2*val;

		System.out.println(score+" "+val+" "+sim.isValid(new Neighbor(new NodeSimple(3), sim.compare(a, b)), 0.97));
	}

	@Override
	public double compare(PointNDBoolean a_, PointNDBoolean b_)
	{
		int[][] m = new int[2][2];
		
	    for (int i = 0; i < a_.size(); i++)
	    {
	    	m[a_.getInt(i)][b_.getInt(i)]++;
	    }
	    
	    return ((double)m[1][1]) / Math.min(m[1][1] + m[0][1], m[1][1] + m[1][0]);
	}

	@Override
	public boolean isValid(Neighbor<TID, PointNDBoolean, TN> neighbor_, double epsilon_)
	{
		return neighbor_.similarity >= epsilon_;
	}

	@Override
	public NeighborListFactory<TID, PointNDBoolean, TN> getNeighborListFactory()
	{
		return new NeighborListFactory<>(new NeighborListComparatorDESC<TID, PointNDBoolean, TN>());
	}

	@Override
	public double similarity(PointNDBoolean a_, PointNDBoolean b_)
	{
		return compare(a_, b_);
	}
}

