package org.apache.gora.example.mapred.temporal;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapperTempAggr extends
    Mapper<LongWritable, Text, Text, FloatWritable> {

  private static Logger Logger = LoggerFactory.getLogger(MapperTempAggr.class);
  private Float sum;
  private int count = 0;
  private Map<String, Float> keys;
  private Map<String, Float> times;

  @Override
  public void setup(Context context) throws IllegalArgumentException,
      IOException {
    Logger.info(". . . Initializing data structure . . .");
    keys = new HashMap<String, Float>();
    times = new HashMap<String, Float>();
    sum = new Float(0.0);
  }

  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    if (count != 0) {
      String[] split = value.toString().trim().split("\\s+");
      try {
        if (split.length > 3) {
          // [CO2 Year&Month Year Month]
          if (!keys.containsKey(split[1])) {
            sum += Float.parseFloat(split[0]);
          } else {
            sum += Float.parseFloat(split[0]) - keys.get(split[0]);
            keys.put(split[0], Float.parseFloat(split[1]));
          }
          times.put(split[1], sum);
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
