package edu.wzm.pattern.join.composite;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

import java.io.IOException;

/**
 * Created by Administrator on 2016/6/19.
 */
public class CompositeMapper extends Mapper<Object, TupleWritable, Text, Text> {

    @Override
    protected void map(Object key, TupleWritable value, Context context) throws IOException, InterruptedException {

        // Get the first two elements in the tuple and output them
        context.write((Text)value.get(0), (Text)value.get(1));
    }
}
