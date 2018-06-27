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
import com.google.common.collect.Iterables;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;

public class Friendster2Edgelist 
{
	public static void main(String[] args_)
	{
		try
		{
			String inputFile = args_[0];
			String outputFile = args_[1];
			
			final FileReader fr = new FileReader(inputFile);
			final LineNumberReader lnr = new LineNumberReader(fr);
			
			final FileOutputStream fileStreamOutput = new FileOutputStream(outputFile, true);
			final PrintStream printOutput = new PrintStream(fileStreamOutput);
			
			String line;
			Splitter splitter = Splitter.on(":");
			Splitter splitterComma = Splitter.on(",");
			
			Set<String> skip = new HashSet<String>();
			skip.add("notfound");
			skip.add("private");
			
			while ((line = lnr.readLine()) != null)
			{
				if (line.startsWith("#"))
				{
					continue;
				}
				
				String[] lineSplitted = Iterables.toArray(splitter.split(line), String.class);
				if(lineSplitted.length==2)
				{
					String[] friend = Iterables.toArray(splitterComma.split(lineSplitted[1]), String.class);
					for(String f : friend)
					{
						if(false == skip.contains(f))
						{
							printOutput.println(lineSplitted[0]+" "+f);
						}
					}
				}
				
			}
			
			lnr.close();
			printOutput.close();
		}
		catch(Exception e_)
		{
			e_.printStackTrace();
		}
	}
}
