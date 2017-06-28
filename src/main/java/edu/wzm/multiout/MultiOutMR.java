package edu.wzm.multiout;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Created by Administrator on 2016/4/24.
 */
public class MultiOutMR {

    public static class MultiOutMapper extends Mapper<Object, Text, Text, IntWritable> {

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

    public static class MultiOutReducer extends Reducer<Text, IntWritable, Text, LongWritable> {

        private LongWritable count = new LongWritable();
        private MultipleOutputs outputs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outputs = new MultipleOutputs(context);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value : values){
                sum += value.get();
            }
            count.set(sum);

            Configuration conf = context.getConfiguration();
            String type = conf.get("type");
            if(type.equalsIgnoreCase("namedOutput")) {
                if(key.toString().equals("hello")) {
                    outputs.write("hello", key, count);
                }
                else {
                    outputs.write("IT", key, count);
                }
            }
            else if(type.equalsIgnoreCase("baseOutputPath")){
                outputs.write(key, count, key.toString());
            }
            else {
                if(key.toString().equals("hello")) {
                    outputs.write("hello", key, count, key.toString());
                }
                else {
                    outputs.write("IT", key, count, key.toString());
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            outputs.close();
        }
    }
}
