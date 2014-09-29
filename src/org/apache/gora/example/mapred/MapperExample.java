package org.apache.gora.example.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapperExample extends Mapper<LongWritable, Text, Text, LongWritable> {

  private Text word = new Text();
  //private LongWritable count = new LongWritable();
  private LongWritable one = new LongWritable(1);
  private static Logger Logger = LoggerFactory.getLogger(MapperExample.class);

  @Override
  public void setup(Context context) throws IllegalArgumentException, IOException {
    Logger.info(". . . Mapping . . .");
  }

  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] split = value.toString().split(" ");
    if (split.length <= 1)
      word.set(value);
    else {
      for(String parts : split) {
        word.set(parts);
        context.write(word, one);
      }
    }
  }

  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    Logger.info(". . . Finalizing map.");
  }
}
