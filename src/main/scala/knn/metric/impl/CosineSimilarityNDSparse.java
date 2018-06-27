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
import knn.util.PointND;
import knn.util.PointNDSparse;

public class CosineSimilarityNDSparse<TID, TN extends INode<TID, PointNDSparse>> implements IMetric<TID, PointNDSparse, TN>
{
	private static final long serialVersionUID = 1L;

	public static void main(String[] args_)
	{
		final CosineSimilarityND sim = new CosineSimilarityND();
		final CosineSimilarityNDSparse sim2 = new CosineSimilarityNDSparse();
		final PointND a = new PointND(new double[]{1,2,1,1,3});
		final PointND b = new PointND(new double[]{1,0,5,0,0});
		
		PointNDSparse c = new PointNDSparse(5);
		PointNDSparse d = new PointNDSparse(2);
		c.add(0, 0, 1);
		c.add(1, 1, 2);
		c.add(2, 2, 1);
		c.add(3, 3, 1);
		c.add(4, 4, 3);
		d.add(0, 0, 1);
		d.add(1, 2, 5);

		System.out.println(sim.compare(a, b)+" "+sim2.compare(c, d));
	}

	@Override
	public double compare(PointNDSparse a_, PointNDSparse b_)
	{
		double dotProduct = 0.0;
	    double normA = 0.0;
	    double normB = 0.0;
	    
	    int indexA = 0;
	    int indexB = 0;
	    
	    while(indexA < a_.size() && indexB < b_.size())
	    {
	    	if(a_.getIndex(indexA) == b_.getIndex(indexB))
	    	{
	    		dotProduct += a_.getValue(indexA) * b_.getValue(indexB);
	    		normA += Math.pow(a_.getValue(indexA), 2);
	    		normB += Math.pow(b_.getValue(indexB), 2);
	    		indexA++;
	    		indexB++;
	    	} else if(a_.getIndex(indexA) < b_.getIndex(indexB))
	    	{
	    		normA += Math.pow(a_.getValue(indexA), 2);
	    		indexA++;
	    	} else
	    	{
	    		normB += Math.pow(b_.getValue(indexB), 2);
	    		indexB++;
	    	}
	    }
	    
	    while(indexA < a_.size())
	    {
	    	normA += Math.pow(a_.getValue(indexA), 2);
    		indexA++;
	    }
	    
	    while(indexB < b_.size())
	    {
	    	normB += Math.pow(b_.getValue(indexB), 2);
    		indexB++;
	    }
	    
	    return (dotProduct / (Math.sqrt(normA) * Math.sqrt(normB)));
	}

	@Override
	public boolean isValid(Neighbor<TID, PointNDSparse, TN> neighbor_, double epsilon_)
	{
		return neighbor_.similarity >= epsilon_;
	}

	@Override
	public NeighborListFactory<TID, PointNDSparse, TN> getNeighborListFactory()
	{
		return new NeighborListFactory<>(new NeighborListComparatorDESC<TID, PointNDSparse, TN>());
	}

	@Override
	public double similarity(PointNDSparse a_, PointNDSparse b_)
	{
		return compare(a_, b_);
	}
}

