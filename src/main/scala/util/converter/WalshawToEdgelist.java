package util.converter;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.util.Iterator;

import com.google.common.base.Splitter;

public class WalshawToEdgelist 
{
	public static void main(String[] args_)
	{
		try
		{
			String inputFile = args_[0];
			String outputFile = args_[1];
			
			final FileReader fr = new FileReader(inputFile);
			final LineNumberReader lnr = new LineNumberReader(fr);
			
			final FileOutputStream fileStreamOutput = new FileOutputStream(outputFile);
			final PrintStream printOutput = new PrintStream(fileStreamOutput);
			
			String line;
			
			boolean firstLineSkipped = false;
			int vertexId = 0;
			Splitter splitter = Splitter.on(" ");

			while ((line = lnr.readLine()) != null)
			{
				if (line.startsWith("#") || line.startsWith("%"))
				{
					continue;
				}
				
				if(!firstLineSkipped)
				{
					firstLineSkipped = true;
					continue;
				}
				
				vertexId ++;
				
				Iterator<String> tokenIterator = splitter.split(line).iterator();
				
				while(tokenIterator.hasNext())
				{
					String token = tokenIterator.next();
					
					if(!token.isEmpty())
						printOutput.println(vertexId+" "+token);
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
