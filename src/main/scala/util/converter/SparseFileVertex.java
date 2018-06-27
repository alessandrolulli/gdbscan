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

import com.google.common.base.Splitter;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

public class SparseFileVertex
{
	public static void main(String[] args_)
	{
		try
		{
			final String inputFile = args_[0];
			final double probability = Double.parseDouble(args_[1]);
			final String outputFile = args_[2];

			final FileReader fr = new FileReader(inputFile);
			final LineNumberReader lnr = new LineNumberReader(fr);

			final FileOutputStream fileStreamOutput = new FileOutputStream(outputFile);
			final PrintStream printOutput = new PrintStream(fileStreamOutput);

			final Random random = new Random();
			final Splitter splitter = Splitter.on(" ");

			String line;

			final Map<String, Boolean> keep = new HashMap<>();

			while ((line = lnr.readLine()) != null)
			{
				if (line.startsWith("#"))
				{
					continue;
				}

				final Iterator<String> tokenIterator = splitter.split(line).iterator();

				boolean print = true;
				while(tokenIterator.hasNext())
				{
					final String s = tokenIterator.next();
					Boolean check = keep.get(s);

					if(check == null)
					{
//						check = (random.nextInt() % probability) != 0;
						check = (random.nextDouble() < probability);
						keep.put(s, check);
					}

					print = print && check;
				}

				if(print)
				{
					printOutput.println(line);
				}
			}

			lnr.close();
			printOutput.close();
		}
		catch(final Exception e_)
		{
			e_.printStackTrace();
		}
	}
}
