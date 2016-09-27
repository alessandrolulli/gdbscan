package util.converter;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

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
