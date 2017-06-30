package edu.wzm.pattern.organization.shuffling;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Created by Administrator on 2016/6/11.
 */
public class ShufflingMR {

    public static class ShufflingMapper extends Mapper<Object, Text, LongWritable, Text>{

        private LongWritable outKey = new LongWritable();
        private Text outValue = new Text();
        private Random random = new Random();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().trim().split(",");
            StringBuilder record = new StringBuilder();
            for(String elem : line){
                if(!elem.startsWith("test")){
                    record.append(elem);
                }
            }
            outKey.set(random.nextLong());
            outValue.set(record.toString());
            context.write(outKey, outValue);
        }
    }

    public static class ShufflingReducer extends Reducer<LongWritable, Text, NullWritable, Text>{

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text val : values){
                context.write(NullWritable.get(), val);
            }
        }
    }
}
