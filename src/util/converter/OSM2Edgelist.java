package util.converter;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class OSM2Edgelist 
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
			
			Set<String> validTagSet = new HashSet<String>();
			validTagSet.add("motorway");
			validTagSet.add("motorway_link");
			validTagSet.add("trunk");
			validTagSet.add("trunk_link");
			validTagSet.add("primary");
			validTagSet.add("primary_link");
			validTagSet.add("secondary");
			validTagSet.add("secondary_link");
			validTagSet.add("tertiary");
			validTagSet.add("tertiary_link");
			validTagSet.add("residential");
			validTagSet.add("service");
			validTagSet.add("unclassified");
			validTagSet.add("road");
			validTagSet.add("living_street");
			
			String line;
			
			List<String> tokenList = new ArrayList<String>();
			boolean checkValidWay = false;

			while ((line = lnr.readLine()) != null)
			{
				if (line.startsWith("#"))
				{
					continue;
				}
				
				if(line.indexOf("</way") != -1)
				{
					//PRINT
					Iterator<String> it = tokenList.iterator();
					if(checkValidWay && it.hasNext())
					{
						String before = it.next();
						
						while(it.hasNext())
						{
							String after = it.next();
							printOutput.println(before+" "+after);
							before = after;
						}
					}
					
					tokenList = new ArrayList<String>();
					continue;
				}
				if(line.indexOf("<way") != -1)
				{
					tokenList = new ArrayList<String>();
					checkValidWay = false;
					continue;
				}
				
				
				if(line.indexOf("<nd") != -1)
				{
					tokenList.add(line.substring(line.indexOf("\"")+1, line.lastIndexOf("\"")));
					continue;
				}
				
				if(!checkValidWay)
				{
					Iterator<String> validTagIterator = validTagSet.iterator();
					
					while(validTagIterator.hasNext() && !checkValidWay)
					{
						if(line.indexOf(validTagIterator.next()) != -1)
						{
							checkValidWay = true;
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
