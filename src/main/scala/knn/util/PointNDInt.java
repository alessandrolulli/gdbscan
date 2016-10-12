package knn.util;


public class PointNDInt implements IPointND
{
	private static final long serialVersionUID = 1L;

	public static PointNDInt NOT_VALID = new PointNDInt(new int[]{});

	private final int[] _point;

	public PointNDInt(int[] point_)
	{
		_point = point_;
	}

	@Override
	public int size()
	{
		return _point.length;
	}

	@Override
	public double get(int i_)
	{
		/*
		 * unchecked...
		 * i hope you know what you are doing
		 */
		return _point[i_];
	}
}
