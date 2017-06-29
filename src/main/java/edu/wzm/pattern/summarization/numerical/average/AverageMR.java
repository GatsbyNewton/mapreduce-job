package edu.wzm.pattern.summarization.numerical.average;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Administrator on 2016/4/3.
 */

/*
* Input file: key,count,average
* 1,2,1.3
* 2,4,2.6
* 2,1,2.4
* 1,2,5.6
* 3,1,8.0
* 2,8,7
* */
public class AverageMR {

    public static class AverageMapper extends Mapper<LongWritable, Text, Text, AverageTuple>{

        private AverageTuple tuple = new AverageTuple();
        private Text outKey = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] line = value.toString().split(",");

            tuple.setCount(Long.parseLong(line[1]));
            tuple.setAverage(Double.parseDouble(line[2]));
            outKey.set(line[0]);

            context.write(outKey, tuple);
        }
    }

    public static class AverageReducer extends Reducer<Text, AverageTuple, Text, AverageTuple>{

        private AverageTuple tuple = new AverageTuple();
        @Override
        protected void reduce(Text key, Iterable<AverageTuple> values, Context context) throws IOException, InterruptedException {

            int count = 0;
            double sum = 0;

            for(AverageTuple value : values){
                count += value.getCount();
                sum += value.getAverage() * value.getCount();
            }

            tuple.setCount(count);
            tuple.setAverage(sum / count);

            context.write(key, tuple);
        }
    }
}
