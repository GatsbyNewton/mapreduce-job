package edu.wzm.pattern.summarization.numerical.mmc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by Administrator on 2016/4/2.
 */

/* Input file: key,date,count
* 123,2015-2-9,1
* 123,2014-9-1,1
* 456,2016-10-9,1
* 789,2016-12-12,1
* 456,2010-6-20,1
* 123,2010-9-1,1
* 789,2017-3-1,1
* */
public class MinMaxCountMR {

    public static class MinMaxCountMapper extends Mapper<LongWritable, Text, Text, MinMaxCountTuple>{

        private Text outKey = new Text();
        private MinMaxCountTuple tuple = new MinMaxCountTuple();
        private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] line = value.toString().trim().split(",");

            String userId = line[0];
            try {
                tuple.setMin(sdf.parse(line[1]));
                tuple.setMax(sdf.parse(line[1]));
                tuple.setCount(Long.parseLong(line[2]));
            }
            catch (ParseException e){
                e.printStackTrace();
            }

            outKey.set(userId);
            context.write(outKey, tuple);
        }
    }

    public static class MinMaxCountReducer extends Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple>{

        private MinMaxCountTuple tuple = new MinMaxCountTuple();
        @Override
        protected void reduce(Text key, Iterable<MinMaxCountTuple> values, Context context) throws IOException, InterruptedException {

            long count = 0;
            for(MinMaxCountTuple value : values){
                if(tuple.getMin() == null ||
                        value.getMin().compareTo(tuple.getMin()) < 0 ){
                    tuple.setMin(value.getMin());
                }

                if(tuple.getMax() == null ||
                        value.getMax().compareTo(tuple.getMax()) > 0){
                    tuple.setMax(value.getMax());
                }

                count += value.getCount();
            }

            tuple.setCount(count);
            context.write(key, tuple);
        }
    }
}
