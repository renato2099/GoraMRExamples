package org.apache.gora.example.temp.mapred;

import java.io.IOException;

import org.apache.gora.example.temp.mapred.mapper.MapperTempAggr;
import org.apache.gora.example.temp.mapred.mapper.MapperTimeSlice;
import org.apache.gora.example.temp.mapred.reducer.ReducerTempAggr;
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

    public static final String TS_COL = "ts";
    public static final String KEY_COL = "key";
    public static final String VAL_COL = "val";
    public static final int SKIP_LINES = 2;
    public static final String TS_VAL = "ts_val";

    private static final String USAGE = "usage: [op] [input] [output] [keyCol] [tsCol] [valCol] [tsVal]";

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(getClass());
        job.setJobName(getClass().getSimpleName());
        // /1 Data/customer.tbl /out2 3 8 0
        Path in = new Path(args[1]);
        Path out = new Path(args[2]);
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        checkOutPath(out, out.getFileSystem(job.getConfiguration()));

        configure(job, args);

        long stime = System.currentTimeMillis();
        boolean jr = job.waitForCompletion(true);
        long etime = System.currentTimeMillis();
        System.out.println("Running time:" + (etime - stime) / 1000);

        return jr ? 0 : 1;
    }

    private void configure(Job job, String args[]) {

        //job column configuration
        job.getConfiguration().set(KEY_COL, args[3]);
        job.getConfiguration().set(TS_COL, args[4]);
        job.getConfiguration().set(VAL_COL, args[5]);

        switch (args[0].charAt(0)) {
            case '0':
                job.setMapperClass(MapperTimeSlice.class);
                // job.setCombinerClass(ReducerTempAggr.class);
                // job.setReducerClass(ReducerTempAggr.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                if (args.length != 7) {
                    System.out.println("Wrong number of parameters." + USAGE);
                    throw new IllegalArgumentException();
                }
                job.getConfiguration().set(TS_VAL, args[6]);
                break;
            case '1':
                job.setMapperClass(MapperTempAggr.class);
                job.setCombinerClass(ReducerTempAggr.class);
                job.setReducerClass(ReducerTempAggr.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(FloatWritable.class);
                break;
            default:
                System.out.println("Operation not supported.");
        }
    }

    private void checkOutPath(Path outputPath, FileSystem fileSystem) {
        try {
            // delete recursively
            if (fileSystem.exists(outputPath))
                fileSystem.delete(outputPath, true);
        } catch (IOException e) {
            System.out.println("- - - Unable to delete output path - - - "
                    + outputPath.toString());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        if (6 < args.length && args.length <= 7) {
            System.out.println(USAGE);
            System.exit(-1);
        }
        int rc = ToolRunner.run(new MapReduceTemporalLauncher(), args);
        System.exit(rc);
    }

    public static boolean verifyLine(String[] split, int kCol, int tCol,
            int vCol) {
        if (split.length > kCol & split.length > tCol & split.length > vCol)
            return true;
        return false;
    }
}
