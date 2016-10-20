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

public class ImageToBinary
{
	public static void main(final String[] args_)
	{
		// 0 -> input
		// 1 -> output
		// 2 -> digit ground truth
		// 3 -> threshold

		try
		{
			String digit = args_[2];
			double threshold = Double.parseDouble(args_[3]);
			
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
				
				while(st.hasMoreTokens())
				{
					double token = Double.parseDouble(st.nextToken());
					builder.append(","+(token >= threshold ? 1 : 0));
				}
				
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
