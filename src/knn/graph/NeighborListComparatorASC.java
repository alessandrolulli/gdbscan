package knn.graph;

import java.io.Serializable;
import java.util.Comparator;

public class NeighborListComparatorASC<TID, T, TN extends INode<TID, T>> implements Comparator<Neighbor<TID, T, TN>>, Serializable
{
	private static final long serialVersionUID = 4245870594502451564L;

	@Override
	public int compare(Neighbor<TID, T, TN> a_, Neighbor<TID, T, TN> b_)
	{
		return b_.compareTo(a_);
	}

}
