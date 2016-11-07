package stats;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import scala.Tuple2;
import scala.Tuple3;

import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by lulli on 07/11/2016.
 */
public class StatsInternalEvaluationAnalyzer
{
    //dataset, epsilon, core, sizeData, sizeDataClustered, clusterNumber, clusterMaxSize, separation, separationWeight, compactness, compactnessWeight

    private enum DataType
    {
        sizeDataClustered(4),
        clusterNumber(5),
        clusterMaxSize(6),
        //separation(7),
        separationWeight(8),
       // compactness(9),
        compactnessWeight(10);

        private int _position;

        private DataType(int position_)
        {
            _position = position_;
        }

        public int getPosition()
        {
            return _position;
        }
    }

    private static final Map<Tuple3<String, Double, Integer>, DescriptiveStatistics> _map = new HashMap<Tuple3<String, Double, Integer>, DescriptiveStatistics>();

    public static DescriptiveStatistics getStats(String dataset_, double epsilon_, int core_)
    {
        DescriptiveStatistics stats = _map.get(new Tuple3<String, Double, Integer>(dataset_, epsilon_, core_));

        if(stats == null)
        {
            stats = new DescriptiveStatistics();
            _map.put(new Tuple3<String, Double, Integer>(dataset_, epsilon_, core_), stats);
        }

        return stats;
    }

    public static void main(String[] args_)
    {
        // args_[0] INPUT
        //DescriptiveStatistics

        for(DataType d : DataType.values()) {
            try {
                int position = d.getPosition();


                final FileReader fr = new FileReader(args_[0]);
                final LineNumberReader lnr = new LineNumberReader(fr);
                String line;

                _map.clear();
                while ((line = lnr.readLine()) != null) {
                    String[] token = line.split(",");

                    DescriptiveStatistics data = getStats(token[0], Double.parseDouble(token[1]), Integer.parseInt(token[2]));
                    data.addValue(Double.parseDouble(token[position]));
                }

                lnr.close();

                for (Map.Entry<Tuple3<String, Double, Integer>, DescriptiveStatistics> i : _map.entrySet()) {
                    System.out.println(d.toString()+","+i.getKey()._1() + "," + i.getKey()._2() + "," + i.getValue().getMean() + "," + i.getValue().getStandardDeviation());
                }

            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
