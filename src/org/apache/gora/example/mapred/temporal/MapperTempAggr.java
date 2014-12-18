package org.apache.gora.example.mapred.temporal;

import static org.apache.gora.example.mapred.temporal.MapReduceTemporalLauncher.KEY_COL;
import static org.apache.gora.example.mapred.temporal.MapReduceTemporalLauncher.TS_COL;
import static org.apache.gora.example.mapred.temporal.MapReduceTemporalLauncher.VAL_COL;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
    if (count > 2) {
       String[] split = Iterables.toArray(Splitter.on("\t").trimResults().omitEmptyStrings().split(value.toString()), String.class);
      try {
        if (verifyLine(split)) {
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

  private boolean verifyLine(String[] split) {
    if (split.length > keyCol & split.length > tsCol & split.length > valCol)
      return true;
    return false;
  }

  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    Logger.info(". . . Finalizing map.");
    for (String time : times.keySet()) {
      context.write(new Text(time), new FloatWritable(times.get(time)));
    }
  }
}
