package org.apache.gora.example.temp.mapred.mapper;

import static org.apache.gora.example.temp.mapred.MapReduceTemporalLauncher.KEY_COL;
import static org.apache.gora.example.temp.mapred.MapReduceTemporalLauncher.TS_COL;
import static org.apache.gora.example.temp.mapred.MapReduceTemporalLauncher.VAL_COL;
import static org.apache.gora.example.temp.mapred.MapReduceTemporalLauncher.SKIP_LINES;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.gora.example.temp.mapred.MapReduceTemporalLauncher;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

public class MapperTempAggr extends
    Mapper<LongWritable, Text, Text, FloatWritable> {

private static Logger Logger = LoggerFactory.getLogger(MapperTempAggr.class);
  private Float sum;
  private int count = 0;
  private Map<String, Float> keys;
  private Map<String, Float> times;
  private int keyCol;
  private int tsCol;
  private int valCol;

  @Override
  public void setup(Context context) throws IllegalArgumentException,
      IOException {
    Logger.info(". . . Initializing data structure . . .");
    keys = new HashMap<String, Float>();
    times = new HashMap<String, Float>();
    sum = new Float(0.0);
    keyCol = Integer.parseInt(context.getConfiguration().get(KEY_COL));
    tsCol = Integer.parseInt(context.getConfiguration().get(TS_COL));
    valCol = Integer.parseInt(context.getConfiguration().get(VAL_COL));
  }

  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    if (count > SKIP_LINES) {
       String[] split = Iterables.toArray(Splitter.on("\t").trimResults().omitEmptyStrings().split(value.toString()), String.class);
      try {
        if (MapReduceTemporalLauncher.verifyLine(split, keyCol, tsCol, valCol)) {
          if (!keys.containsKey(split[keyCol])) {
            sum += Float.parseFloat(split[valCol]);
          } else {
            sum += Float.parseFloat(split[valCol]) - keys.get(split[keyCol]);
          }
          keys.put(split[keyCol], Float.parseFloat(split[valCol]));
          times.put(split[tsCol], sum);
        }
      } catch (Exception e) {
        for (String p : split)
          Logger.info(p + " - ");
        e.printStackTrace();
      }
    }
    count++;
  }

  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    Logger.info(". . . Finalizing map.");
    for (String time : times.keySet()) {
      context.write(new Text(time), new FloatWritable(times.get(time)));
    }
  }
}
