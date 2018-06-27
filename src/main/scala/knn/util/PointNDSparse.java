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
