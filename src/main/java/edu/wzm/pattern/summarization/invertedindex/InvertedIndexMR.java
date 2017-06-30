package edu.wzm.pattern.summarization.invertedindex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Administrator on 2016/4/4.
 */
public class InvertedIndexMR {

    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text>{

        private Text links = new Text();
        private Text outKey = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] line = value.toString().split(",");
            outKey.set(line[0]);
            links.set(line[1]);

            context.write(links, outKey);
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>{

        private Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for(Text value : values){
                if(first){
                    first = false;
                }
                else {
                    sb.append("");
                }
                sb.append(value);
            }
            outValue.set(sb.toString());

            context.write(key, outValue);
        }
    }
}
