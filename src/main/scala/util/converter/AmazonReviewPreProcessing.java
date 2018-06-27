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

public class AmazonReviewPreProcessing
{
	public static void main(final String[] args_)
	{
		// 0 -> input
		// 1 -> output

//		product/productId: B006K2ZZ7K
//		review/userId: A3JRGQVEQN31IQ
//		review/profileName: Pamela G. Williams
//		review/helpfulness: 0/0
//		review/score: 5.0
//		review/time: 1336003200
//		review/summary: Wonderful, tasty taffy
//		review/text: This taffy is so good.  It is very soft and chewy.  The flavors are amazing.  I would definitely recommend you buying it.  Very satisfying!!
		
		try
		{
			final FileOutputStream fileEdgeList = new FileOutputStream(args_[1]);
			final PrintStream printEdgeList = new PrintStream(fileEdgeList);

			final FileReader fr = new FileReader(args_[0]);
			final LineNumberReader lnr = new LineNumberReader(fr);
			String line;

			String productId = "";
			String userId = "";
			String text = "";
			int index = 0;
			while ((line = lnr.readLine()) != null)
			{
				if(line.contains("productId"))
				{
					productId = line.substring(19);
				} else if(line.contains("userId"))
				{
					userId = line.substring(15);
				} else if(line.contains("text"))
				{
					text = line.substring(13).toLowerCase();
					text = text.replace(",", "");
					text = text.replace("<br />", "");
					
					printEdgeList.println(index+","+productId+","+userId+","+text);
					index++;
				}
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
