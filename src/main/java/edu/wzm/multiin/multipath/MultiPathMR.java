package edu.wzm.multiin.multipath;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by wangzhimingissac on 2016/3/28.
 */
public class MultiPathMR {

    public static class MultiMap1 extends Mapper<LongWritable, Text, Text, IntWritable>{
        private Text outKey = new Text();
        private IntWritable outValue = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().trim().split(",");
            for(String word : line){
                outKey.set(word);
                context.write(outKey, outValue);
            }
        }
    }

    public static class MultiMap2 extends Mapper<LongWritable, Text, Text, IntWritable>{
        private Text outKey = new Text();
        private IntWritable outValue = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().trim().split("\t");
            for(String word : line){
                outKey.set(word);
                context.write(outKey, outValue);
            }
        }
    }

    public static class MultiReduce extends Reducer<Text, IntWritable, Text, LongWritable>{
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
