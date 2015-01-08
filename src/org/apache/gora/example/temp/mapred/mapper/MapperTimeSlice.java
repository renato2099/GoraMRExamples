package org.apache.gora.example.temp.mapred.mapper;

import static org.apache.gora.example.temp.mapred.MapReduceTemporalLauncher.KEY_COL;
import static org.apache.gora.example.temp.mapred.MapReduceTemporalLauncher.SKIP_LINES;
import static org.apache.gora.example.temp.mapred.MapReduceTemporalLauncher.TS_COL;
import static org.apache.gora.example.temp.mapred.MapReduceTemporalLauncher.TS_DELIM;
import static org.apache.gora.example.temp.mapred.MapReduceTemporalLauncher.TS_VAL;
import static org.apache.gora.example.temp.mapred.MapReduceTemporalLauncher.VAL_COL;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.gora.example.temp.mapred.MapReduceTemporalLauncher;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

public class MapperTimeSlice extends Mapper<LongWritable, Text, Text, Text> {

    private static Logger Logger = LoggerFactory
            .getLogger(MapperTimeSlice.class);
    private int count = 0;
    private Map<String, String> keys;
    private int keyCol;
    private int tsCol;
    private int valCol;
    private int tsVal;

    @Override
    public void setup(Context context) throws IllegalArgumentException,
            IOException {
        Logger.info(". . . Initializing data structure . . .");
        keys = new HashMap<String, String>();
        keyCol = Integer.parseInt(context.getConfiguration().get(KEY_COL));
        tsCol = Integer.parseInt(context.getConfiguration().get(TS_COL));
        valCol = Integer.parseInt(context.getConfiguration().get(VAL_COL));
        tsVal = Integer.parseInt(context.getConfiguration().get(TS_VAL));
    }

    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        if (count > SKIP_LINES) {
            String[] split = Iterables.toArray(Splitter.on("\t").trimResults()
                    .omitEmptyStrings().split(value.toString()), String.class);
            try {
                if (MapReduceTemporalLauncher.verifyLine(split, keyCol, tsCol,
                        valCol)) {
                    // In this case as the schema is fixed then new tuples can
                    // simply replace old ones. No need to aggregate past ones
                    if (tsVal >= Float.parseFloat(split[tsCol])) {
                        if (!keys.containsKey(split[keyCol])) {
                            keys.put(split[keyCol], split[tsCol] + TS_DELIM
                                    + value.toString());
                        } else {
                            String prevTs = Splitter.on(TS_DELIM)
                                    .omitEmptyStrings()
                                    .split(keys.get(split[keyCol])).iterator()
                                    .next();
                            if (Float.parseFloat(prevTs) < Float
                                    .parseFloat(split[tsCol])) {
                                // pre-aggr per partition
                                keys.put(split[keyCol], split[tsCol] + TS_DELIM
                                        + value.toString());
                            }
                        }
                    }
                }
            } catch (Exception e) {
                for (String p : split)
                    Logger.error(p + " - ");
                e.printStackTrace();
            }
        }
        count++;
    }

    @Override
    public void cleanup(Context context) throws IOException,
            InterruptedException {
        for (Entry<String, String> entry : keys.entrySet()) {
            context.write(new Text(entry.getKey()), new Text(entry.getValue()));
        }
    }
}
