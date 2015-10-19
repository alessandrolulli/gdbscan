package util.converter;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

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
