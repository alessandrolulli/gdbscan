package knn.util;

import java.io.Serializable;

public class Point2D implements Serializable 
{
	private static final long serialVersionUID = 1L;

	public static Point2D NOT_VALID = new Point2D(-1, -1);
	
	private final double _x;
	private final double _y;
	
	public Point2D(double x_, double y_)
	{
		_x = x_;
		_y = y_;
	}
	
	public double getX()
	{
		return _x;
	}
	
	public double getY()
	{
		return _y;
	}

}
