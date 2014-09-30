package org.apache.gora.example.mapred.temporal;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapReduceTemporalLauncher extends Configured implements Tool {

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(new Configuration());
    job.setJarByClass(getClass());
    job.setJobName(getClass().getSimpleName());

    Path in = new Path(args[0]);
    Path out = new Path(args[1]);
    FileInputFormat.addInputPath(job, in);
    FileOutputFormat.setOutputPath(job, out);
    checkOutPath(out, out.getFileSystem(job.getConfiguration()));

    job.setMapperClass(MapperTempAggr.class);
    //job.setCombinerClass(ReducerExample.class);
    //job.setReducerClass(ReducerExample.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  private void checkOutPath(Path outputPath, FileSystem fileSystem) {
    try {
      // delete recursively
      if (fileSystem.exists(outputPath))
        fileSystem.delete(outputPath, true);
    } catch (IOException e) {
      System.out.println("- - - Unable to delete output path - - - " + outputPath.toString());
      e.printStackTrace();
    }
    
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.println("usage: [input] [output]");
      System.exit(-1);
    }
    int rc = ToolRunner.run(new MapReduceTemporalLauncher(), args);
    System.exit(rc);
  }

}
