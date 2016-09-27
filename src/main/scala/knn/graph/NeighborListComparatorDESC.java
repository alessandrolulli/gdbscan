package knn.graph;

import java.io.Serializable;
import java.util.Comparator;

public class NeighborListComparatorDESC<TID, T, TN extends INode<TID, T>> implements Comparator<Neighbor<TID, T, TN>>, Serializable
{
	private static final long serialVersionUID = -8502127152592156959L;

	@Override
	public int compare(Neighbor<TID, T, TN> a_, Neighbor<TID, T, TN> b_)
	{
		return a_.compareTo(b_);
	}

}
