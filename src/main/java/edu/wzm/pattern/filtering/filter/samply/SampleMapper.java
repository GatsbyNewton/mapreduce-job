package edu.wzm.pattern.filtering.filter.samply;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;

/**
 * Created by Administrator on 2016/4/7.
 */
public class SampleMapper extends Mapper<Object, Text, NullWritable, Text>{

    private Random rand = new Random();
    private Double percentage;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String strPercentage = conf.get("percentage");
        percentage = Double.parseDouble(strPercentage) / 1.0;
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        if(rand.nextDouble() < percentage){
            context.write(NullWritable.get(), value);
        }
    }
}
