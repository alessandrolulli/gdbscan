package util;

public class Main
{
	public static void main(String[] args_)
	{
		if(args_.length > 0)
		{
			enn.densityBased.ENNMainScala.main(args_);
		} else
		{
			System.out.println("ERROR Command input must be: command configFile");
		}
	}
}
