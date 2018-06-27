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

import java.io.*;
import java.util.StringTokenizer;

public class ImageToBinary
{
	public static void main(final String[] args_)
	{
		// 0 -> input
		// 1 -> output
		// 2 -> digit ground truth
		// 3 -> threshold
		// 4 -> pruning

		try
		{
			String digit = args_[2];
			double threshold = Double.parseDouble(args_[3]);
			int pruning = Integer.parseInt(args_[4]);
			
			final FileOutputStream fileEdgeList = new FileOutputStream(args_[1]);
			final PrintStream printEdgeList = new PrintStream(fileEdgeList);

			final FileReader fr = new FileReader(args_[0]);
			final LineNumberReader lnr = new LineNumberReader(fr);
			String line;

			while ((line = lnr.readLine()) != null)
			{
				final StringTokenizer st = new StringTokenizer(line, ",");
				StringBuilder builder = new StringBuilder();
				
				builder.append(digit);
				
				int count = 0;
				while(st.hasMoreTokens())
				{
					double token = Double.parseDouble(st.nextToken());
					boolean valid = token >= threshold;
					builder.append(","+(valid ? 1 : 0));
					if(valid) count++;
				}
				
				if(count > pruning)
					printEdgeList.println(builder.toString());
			}

			printEdgeList.flush();
			printEdgeList.close();
			lnr.close();
		} catch (final IOException e)
		{
			throw new RuntimeException(e);
		}
	}
}
