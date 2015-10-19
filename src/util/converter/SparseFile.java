package util.converter;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.util.Random;

public class SparseFile 
{
	public static void main(String[] args_)
	{
		try
		{
			String inputFile = args_[0];
			int probability = Integer.parseInt(args_[1]);
			String outputFile = args_[2];
			
			final FileReader fr = new FileReader(inputFile);
			final LineNumberReader lnr = new LineNumberReader(fr);
			
			final FileOutputStream fileStreamOutput = new FileOutputStream(outputFile);
			final PrintStream printOutput = new PrintStream(fileStreamOutput);
			
			Random random = new Random();
			
			String line;

			while ((line = lnr.readLine()) != null)
			{
				if (line.startsWith("#"))
				{
					continue;
				}
				
				if(random.nextInt() % probability == 0)
				{
					printOutput.println(line);
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
