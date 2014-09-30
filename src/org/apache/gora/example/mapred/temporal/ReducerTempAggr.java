package org.apache.gora.example.mapred.temporal;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReducerTempAggr<KEY> extends
    Reducer<KEY, FloatWritable, KEY, FloatWritable> {

  private FloatWritable result = new FloatWritable();
  private static Logger Logger = LoggerFactory.getLogger(ReducerTempAggr.class);

  @Override
  public void setup(Context context) throws IllegalArgumentException,
      IOException {
    Logger.info(". . . Reducing . . .");
  }

  public void reduce(KEY key, Iterable<FloatWritable> values, Context context)
      throws IOException, InterruptedException {
    long sum = 0;

    for (FloatWritable val : values) {
      sum += val.get();
    }
    result.set(sum);
    context.write(key, result);
  }

  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    Logger.info(". . . Finalizing reduce.");
  }

}
