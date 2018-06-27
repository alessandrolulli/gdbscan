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

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.*;

public class GeneratePath 
{
	public static void main(String[] args_)
	{
		try
		{
			int n = Integer.parseInt(args_[0]);
			String outputFile = args_[1];
			
			List<Integer> vertices = new ArrayList<Integer>(n);
			for (int i = 0 ; i < n ; i ++)
			{
				vertices.add(i);
			}
			
			long seed = System.nanoTime();
			Collections.shuffle(vertices, new Random(seed));
			
			final FileOutputStream fileStreamOutput = new FileOutputStream(outputFile);
			final PrintStream printOutput = new PrintStream(fileStreamOutput);
			
			String line;
			
			Iterator<Integer> it = vertices.iterator();
			int previous = -1;

			while (it.hasNext())
			{
				int next = it.next();
				
				if(previous != -1)
				{
					printOutput.println(previous+" "+next);
				}
				
				previous = next;
			}
			
			printOutput.close();
		}
		catch(Exception e_)
		{
			e_.printStackTrace();
		}
	}
}
