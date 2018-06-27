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

package util.converter;

import com.google.common.base.Optional;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import java.io.*;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;

public class EdgelistFromAlphabetToNumber
{
	private static final AtomicInteger _incrementalId = new AtomicInteger(0);
	private static final BiMap<String, Integer> _vertexMapping = HashBiMap.create();

	public static void main(final String[] args_)
	{
		// 0 -> input
		// 1 -> output

		try
		{
			final FileOutputStream fileEdgeList = new FileOutputStream(args_[1]);
			final PrintStream printEdgeList = new PrintStream(fileEdgeList);

			final FileReader fr = new FileReader(args_[0]);
			final LineNumberReader lnr = new LineNumberReader(fr);
			String line;
			boolean skipFirstLine = true;

			while ((line = lnr.readLine()) != null)
			{
				if(skipFirstLine)
				{
					skipFirstLine = false;
					continue;
				}
				
				if (line.startsWith("#"))
				{
					continue;
				}
				final StringTokenizer st = new StringTokenizer(line);

				if (!st.hasMoreTokens() || (st.countTokens() < 2))
				{
					continue;
				}

				final int vertexA = getVertexId(st.nextToken());
				final int vertexB = getVertexId(st.nextToken());

				printEdgeList.println(vertexA+" "+vertexB);
			}

			printEdgeList.flush();
			printEdgeList.close();
			lnr.close();
		} catch (final IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	public static int getVertexId(final String val_)
	{
		final Optional<Integer> id = Optional.fromNullable(_vertexMapping.get(val_));

		if(id.isPresent())
		{
			return id.get();
		} else
		{
			final int nextId = _incrementalId.getAndIncrement();
			_vertexMapping.put(val_, nextId);

			return nextId;
		}
	}
}
