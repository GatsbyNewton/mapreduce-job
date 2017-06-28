package edu.wzm.multiin.setpath;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Administrator on 2016/4/24.
 */
public class SetPathMR {

    public static class SetPathMapper extends Mapper<Object, Text, Text, IntWritable>{

        private Text outKey = new Text();
        private IntWritable outValue = new IntWritable(1);
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().trim().split(",");
            for(String word : line){
                outKey.set(word);
                context.write(outKey, outValue);
            }
        }
    }

    public static class SetPathReducer extends Reducer<Text, IntWritable, Text, LongWritable>{
        private LongWritable count = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value : values){
                sum += value.get();
            }
            count.set(sum);
            context.write(key, count);
        }
    }
}
