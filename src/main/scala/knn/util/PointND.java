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

package knn.util;


public class PointND implements IPointND
{
	private static final long serialVersionUID = 1L;

	public static PointND NOT_VALID = new PointND(new double[]{});

	private final double[] _point;

	public PointND(double[] point_)
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
