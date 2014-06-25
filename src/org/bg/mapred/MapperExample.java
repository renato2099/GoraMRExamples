package org.bg.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperExample extends Mapper<LongWritable, Text, Text, LongWritable> {

  private Text word = new Text();
  //private LongWritable count = new LongWritable();
  private LongWritable one = new LongWritable(1);
 
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
}
