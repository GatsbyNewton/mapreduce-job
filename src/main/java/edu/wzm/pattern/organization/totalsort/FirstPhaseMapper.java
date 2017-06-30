package edu.wzm.pattern.organization.totalsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Administrator on 2016/6/11.
 */
public class FirstPhaseMapper extends Mapper<Object, Text, LongWritable, Text>{

    private LongWritable outKey = new LongWritable();
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split(",");
        outKey.set(Long.parseLong(line[0]));
        context.write(outKey, value);
    }
}
