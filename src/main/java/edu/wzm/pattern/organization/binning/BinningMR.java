package edu.wzm.pattern.organization.binning;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Created by Administrator on 2016/6/11.
 */
public class BinningMR extends Mapper<Object, Text, NullWritable, Text> {

    private MultipleOutputs mos = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.mos = new MultipleOutputs(context);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split(",");
        if(line[0].equalsIgnoreCase("hadoop")){
            mos.write("bins", NullWritable.get(), value, "hadoop-tag");
        }
        else if(line[0].equalsIgnoreCase("hive")){
            mos.write("bins", NullWritable.get(), value, "hive-tag");
        }
        else if(line[0].equalsIgnoreCase("spark")){
            mos.write("bins", NullWritable.get(), value, "spark-tag");
        }
        else {
            mos.write("bins", NullWritable.get(), value, "other-tag");
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
