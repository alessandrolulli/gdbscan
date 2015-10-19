package knn.metric;

import knn.graph.IMetric;
import knn.graph.INode;
import knn.graph.Neighbor;
import knn.graph.NeighborListComparatorASC;
import knn.graph.NeighborListFactory;
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
		return 1 - compare(a_, b_);
	}

}
