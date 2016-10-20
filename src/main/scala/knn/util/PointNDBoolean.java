package knn.util;

import java.io.Serializable;


public class PointNDBoolean implements Serializable
{
	private static final long serialVersionUID = 1L;

	public static PointNDBoolean NOT_VALID = new PointNDBoolean(new boolean[]{});

	private final boolean[] _point;

	public PointNDBoolean(boolean[] point_)
	{
		_point = point_;
	}

	public int size()
	{
		return _point.length;
	}

	public boolean get(int i_)
	{
		/*
		 * unchecked...
		 * i hope you know what you are doing
		 */
		return _point[i_];
	}
	
	public int getInt(int i_)
	{
		return get(i_) ? 1 : 0;
	}
}
