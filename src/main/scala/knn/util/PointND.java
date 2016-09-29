package knn.util;

import java.io.Serializable;

public class PointND implements Serializable
{
	private static final long serialVersionUID = 1L;

	public static PointND NOT_VALID = new PointND(new double[]{});

	private final double[] _point;

	public PointND(double[] point_)
	{
		_point = point_;
	}

	public int size()
	{
		return _point.length;
	}

	public double get(int i_)
	{
		/*
		 * unchecked...
		 * i hope you know what you are doing
		 */
		return _point[i_];
	}

	public boolean getBoolean(int i_)
	{
		/*
		 * unchecked...
		 * i hope you know what you are doing
		 */
		return _point[i_] > 0;
	}

	public int getValidPosition()
	{
		int toReturn = 0;

		for(int i = 0 ; i < size() ; i++)
		{
			toReturn += get(i) > 0 ? 1 : 0;
		}

		return toReturn;
	}

	public double getSum()
	{
		double toReturn = 0;

		for(int i = 0 ; i < size() ; i++)
		{
			toReturn += get(i);
		}

		return toReturn;
	}

	public double[] sumToDouble(PointND other_)
	{
		final double[] toReturn = new double[size()];

		for(int i = 0 ; i < size() ; i++)
		{
			toReturn[i] = get(i) + other_.get(i);
		}

		return toReturn;
	}

	public PointND sum(PointND other_)
	{
		return new PointND(sumToDouble(other_));
	}

	public double[] divToDouble(int val_)
	{
		final double[] toReturn = new double[size()];

		for(int i = 0 ; i < size() ; i++)
		{
//			toReturn[i] = get(i) / val_;
			toReturn[i] = Math.round((get(i) / val_) * 100000d) / 100000d;
		}

		return toReturn;
	}
}
