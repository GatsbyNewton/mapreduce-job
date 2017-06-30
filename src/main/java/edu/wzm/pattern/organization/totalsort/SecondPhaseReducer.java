package edu.wzm.pattern.organization.totalsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Administrator on 2016/6/11.
 */
public class SecondPhaseReducer extends Reducer<LongWritable, Text, NullWritable, Text>{

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text val : values){
            context.write(NullWritable.get(), val);
        }
    }
}
