package edu.wzm.pattern.filtering.distinct;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Administrator on 2016/4/10.
 */
public class DistinctMR {

    public static class DistinctMapper extends Mapper<Object, Text, Text, NullWritable>{

        private Text uid = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] line = value.toString().split(",");
            uid.set(line[0]);
            context.write(uid, NullWritable.get());
        }
    }

    public static class DistinctReducer extends Reducer<Text, NullWritable, Text, NullWritable>{

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            context.write(key, NullWritable.get());
        }
    }
}
