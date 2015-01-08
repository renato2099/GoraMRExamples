package org.apache.gora.example.temp.mapred.reducer;

import static org.apache.gora.example.temp.mapred.MapReduceTemporalLauncher.TS_DELIM;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;

public class ReducerTimeSlice<KEY> extends Reducer<KEY, Text, KEY, Text> {

    private static Logger Logger = LoggerFactory
            .getLogger(ReducerTimeSlice.class);

    @Override
    public void setup(Context context) throws IllegalArgumentException,
            IOException {
        Logger.info(". . . Reducing . . .");
    }

    public void reduce(KEY key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String prevTs = "0";
        Text endVal = new Text();
        for (Text val : values) {
            Iterator<String> iterator = Splitter.on(TS_DELIM)
                    .omitEmptyStrings().split(val.toString()).iterator();
            if (Float.parseFloat(prevTs) < Float.parseFloat(iterator.next())) {
                prevTs = Splitter.on(TS_DELIM).omitEmptyStrings()
                        .split(val.toString()).iterator().next();
                endVal.set(iterator.next());
            }
        }
        context.write(null, endVal);
    }

    @Override
    public void cleanup(Context context) throws IOException,
            InterruptedException {
        Logger.info(". . . Finalizing reduce.");
    }

}
