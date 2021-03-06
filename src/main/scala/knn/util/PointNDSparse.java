package knn.util;

import java.io.Serializable;

public class PointNDSparse implements Serializable
{
	private final int[] _index;
	private final int[] _value;
	
	public PointNDSparse(int size_)
	{
		_index = new int[size_];
		_value = new int[size_];
	}
	
	public void add(int position_, int index_, int value_)
	{
		_index[position_] = index_;
		_value[position_] = value_;
	}
	
	public int getIndex(int position_)
	{
		return _index[position_];
	}
	
	public int getValue(int position_)
	{
		return _value[position_];
	}
	
	public int size()
	{
		return _index.length;
	}
	
	public String toString()
	{
		StringBuilder b = new StringBuilder();
		
		for(int i = 0 ; i < size() ; i++)
		{
			b.append(getIndex(i)+" "+getValue(i)+"|||");
		}
		
		return b.toString();
	}
}
