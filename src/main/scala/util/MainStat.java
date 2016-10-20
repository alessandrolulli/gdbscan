package util;

public class MainStat
{
	public static void main(String[] args_)
	{
		if(args_.length > 0)
		{
			stats.DatasetStats.main(args_);
		} else
		{
			System.out.println("ERROR Command input must be: command configFile");
		}
	}
}
