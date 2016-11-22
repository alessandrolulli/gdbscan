package stats;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import scala.Tuple2;

import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by lulli on 07/11/2016.
 */
public class StatsAnalyzer
{

    private static final Map<Tuple2<String, Double>, DescriptiveStatistics> _map = new HashMap<Tuple2<String, Double>, DescriptiveStatistics>();
    private static final Map<Tuple2<String, Double>, DescriptiveStatistics> _mapSecond = new HashMap<Tuple2<String, Double>, DescriptiveStatistics>();

    public static DescriptiveStatistics getStats(String dataset_, double epsilon_)
    {
        DescriptiveStatistics stats = _map.get(new Tuple2<String, Double>(dataset_, epsilon_));

        if(stats == null)
        {
            stats = new DescriptiveStatistics();
            _map.put(new Tuple2<String, Double>(dataset_, epsilon_), stats);
        }

        return stats;
    }

    public static DescriptiveStatistics getStatsSecond(String dataset_, double epsilon_)
    {
        DescriptiveStatistics stats = _mapSecond.get(new Tuple2<String, Double>(dataset_, epsilon_));

        if(stats == null)
        {
            stats = new DescriptiveStatistics();
            _mapSecond.put(new Tuple2<String, Double>(dataset_, epsilon_), stats);
        }

        return stats;
    }

    public static void main(String[] args_)
    {
        // args_[0] INPUT
        //DescriptiveStatistics

        try
        {
            final FileReader fr = new FileReader(args_[0]);
            final LineNumberReader lnr = new LineNumberReader(fr);
            String line;

            String index = "-1";
            if(args_[1] != null)
            {
                index = args_[1];
            }

            while ((line = lnr.readLine()) != null)
            {
                String[] token = line.split(",");

                if(token[0].equals("ENN") && token[3].equals(index))
                {
                    DescriptiveStatistics stats = getStats(token[1], Double.parseDouble(token[12]));

                    stats.addValue(Integer.parseInt(token[4]));
                } else if(token[0].equals("CRACKER_DENSITY"))
                {
                    DescriptiveStatistics stats = getStatsSecond(token[1], Double.parseDouble(token[13]));

                    stats.addValue(Integer.parseInt(token[4]));
                }
            }

            lnr.close();

            for(Map.Entry<Tuple2<String, Double>, DescriptiveStatistics> i : _map.entrySet())
            {
                System.out.println("FIRST: "+i.getKey()._1()+","+i.getKey()._2()+","+i.getValue().getMean()+","+i.getValue().getStandardDeviation());
            }
            for(Map.Entry<Tuple2<String, Double>, DescriptiveStatistics> i : _mapSecond.entrySet())
            {
                System.out.println("SECOND: "+i.getKey()._1()+","+i.getKey()._2()+","+i.getValue().getMean()+","+i.getValue().getStandardDeviation());
            }

        } catch (final IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
