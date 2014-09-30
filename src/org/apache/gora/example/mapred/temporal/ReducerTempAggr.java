package org.apache.gora.example.mapred.temporal;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class ReducerTempAggr<KEY> extends
    Reducer<KEY, LongWritable, KEY, LongWritable> {

  private LongWritable result = new LongWritable();
  private static Logger Logger = LoggerFactory.getLogger(ReducerTempAggr.class);

  @Override
  public void setup(Context context) throws IllegalArgumentException,
      IOException {
    Logger.info(". . . Reducing . . .");
  }

  public void reduce(KEY key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
    long sum = 0;

    for (LongWritable val : values) {
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
