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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author tibo
 */
public class NeighborList<TID, T, TN extends INode<TID, T>> extends BoundedPriorityQueue<Neighbor<TID, T, TN>> implements Serializable {

	private static final long serialVersionUID = 1969183297395875761L;

	public NeighborList(List<Neighbor<TID, T, TN>> list_)
    {
        super(list_.size() > 0 ? list_.size() : 1, new NeighborListComparatorDESC<TID, T, TN>());

        for(final Neighbor<TID, T, TN> n : list_)
        {
        	add(n);
        }
    }

    public NeighborList(int size)
    {
        super(size, new NeighborListComparatorDESC<TID, T, TN>());
    }

    public NeighborList(int size, boolean inverted_)
    {
        super(size, inverted_ ? new NeighborListComparatorASC<TID, T, TN>() : new NeighborListComparatorDESC<TID, T, TN>());
    }

    public NeighborList(int size, Comparator<Neighbor<TID, T, TN>> comparator_)
    {
        super(size, comparator_);
    }

    public NeighborList<TID, T, TN> convertWithSize(int size_)
    {
        if(size_ < size()) {
            final NeighborList<TID, T, TN> toReturn = new NeighborList<TID, T, TN>(size_);

            toReturn.addAll(this);

            return toReturn;
        } else return this;
    }

    public ArrayList<Neighbor<TID, T, TN>> convertToList()
    {
    	final ArrayList<Neighbor<TID, T, TN>> list = new ArrayList<Neighbor<TID, T, TN>>();
    	final Iterator<Neighbor<TID, T, TN>> it = iterator();
    	while(it.hasNext())
    	{
    		final Neighbor next = it.next();
    		list.add(next);
    	}

    	return list;
    }

    public Neighbor getMinSimilarity()
    {
    	Neighbor toReturn = null;
    	for(final Neighbor n : convertToList())
    	{
    		if((toReturn == null) || (n.similarity < toReturn.similarity))
    		{
    			toReturn = n;
    		}
    	}
    	return toReturn;
    }

    public double getAvgSimilarity()
    {
    	double sum = 0;
    	for(final Neighbor n : convertToList())
    	{
    		sum += n.similarity;
    	}
    	return sum / size();
    }

    public Neighbor getMaxSimilarity()
    {
    	Neighbor toReturn = null;
    	for(final Neighbor n : convertToList())
    	{
    		if((toReturn == null) || (n.similarity > toReturn.similarity))
    		{
    			toReturn = n;
    		}
    	}
    	return toReturn;
    }

    public String getNeighbourId()
    {
    	final List<Neighbor<TID, T, TN>> list = convertToList();
    	final StringBuilder toReturn = new StringBuilder();
    	for(final Neighbor<TID, T, TN> n : list)
    	{
    		toReturn.append(n.node.getId());
    		toReturn.append(" ");
    		toReturn.append(n.similarity);
    		toReturn.append(" ");
    	}

    	return toReturn.toString();
    }

    /**
     * Count common values between this NeighborList and the other.
     * Both neighborlists are not modified.
     *
     * @param other_nl
     * @return
     */
    public int CountCommonValues(NeighborList<TID, T, TN> other_nl) {
        //NeighborList copy = (NeighborList) other.clone();
        final ArrayList other_values = new ArrayList();
        for (final Neighbor n : other_nl) {
            other_values.add(n.node.getValue());
        }

        int count = 0;
        for (final Neighbor n : this) {
            final Object this_value = n.node.getValue();

            for (final Object other_value : other_values) {
                if ( other_value.equals(this_value)) {
                    count++;
                    other_values.remove(other_value);
                    break;
                }
            }
        }

        return count;
    }
}
