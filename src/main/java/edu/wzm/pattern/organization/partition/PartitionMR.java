package edu.wzm.pattern.organization.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by Administrator on 2016/6/1.
 */
public class PartitionMR {

    public static class PartitionMapper extends Mapper<Object, Text, IntWritable, Text>{
        private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        private IntWritable outKey = new IntWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().trim().split(",");
            try {
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(sdf.parse(line[0]));
                outKey.set(calendar.get(Calendar.YEAR));
                context.write(outKey, value);
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public static class PartitionReducer extends Reducer<IntWritable, Text, NullWritable, Text>{

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text val : values){
                context.write(NullWritable.get(), val);
            }
        }
    }
}
