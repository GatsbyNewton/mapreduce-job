package edu.wzm.pattern.summarization.numerical.median;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by Administrator on 2016/4/4.
 */
public class MedianStdDevMR {

    public static class MedianDeviationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

        private Text outKey = new Text();
        private DoubleWritable outValue = new DoubleWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] line = value.toString().split(",");
            outKey.set(line[0]);
            outValue.set(Double.parseDouble(line[1]));

            context.write(outKey, outValue);
        }
    }

    public static class MedianDeviationReducer extends Reducer<Text, DoubleWritable, Text, MedianStdDevTuple>{

        private MedianStdDevTuple tuple = new MedianStdDevTuple();
        private List<Double> comments = new ArrayList<Double>();
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

            int count = 0;
            double sum = 0;

            for(DoubleWritable value : values){
                comments.add(value.get());
                count++;
                sum += value.get();
            }

            // Sort comments to calculate median
            Collections.sort(comments);

            // if comments is an even value, average middle two element.
            if(count % 2 == 0){
                double median = (comments.get(count / 2 - 1) + comments.get(count / 2)) / 2;
                tuple.setMedian(median);
            }
            else {
                tuple.setMedian(comments.get(count / 2));
            }

            // Calculate standard deviation
            double mean = sum / count;
            double sumOfSquares = 0;
            for(double comment : comments){
                sumOfSquares += (comment - mean) * (comment - mean);
            }
            tuple.setStdDev(Math.sqrt(sumOfSquares) / (count - 1));

            context.write(key, tuple);
        }
    }
}
