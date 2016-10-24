package util.converter;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Optional;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

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
