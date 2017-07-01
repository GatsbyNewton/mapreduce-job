package edu.wzm.pattern.filtering.filter.grep;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Administrator on 2016/4/7.
 */
public class GrepMapper extends Mapper<Object, Text, NullWritable, Text>{

    private String mapRegex = null;
    private Text txt = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        mapRegex = conf.get("mapRegex");
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");

        for(String val : line){
            if(val.matches(mapRegex)){
                txt.set(val);
                context.write(NullWritable.get(), txt);
            }
        }
    }
}
