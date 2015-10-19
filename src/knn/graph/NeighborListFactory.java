package knn.graph;

import java.io.Serializable;
import java.util.Comparator;

public class NeighborListFactory<TID, T, TN extends INode<TID, T>> implements Serializable
{
	private static final long serialVersionUID = 3637625262337648884L;

	private final Comparator<Neighbor<TID, T, TN>> _comparator;

	public NeighborListFactory()
	{
		_comparator = new NeighborListComparatorDESC<TID, T, TN>();
	}

	public NeighborListFactory(Comparator<Neighbor<TID, T, TN>> comparator_)
	{
		_comparator = comparator_;
	}

	public NeighborList<TID, T, TN> create(int size_)
	{
		return new NeighborList<TID, T, TN>(size_, _comparator);
	}

}
