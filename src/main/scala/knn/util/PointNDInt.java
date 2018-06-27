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
