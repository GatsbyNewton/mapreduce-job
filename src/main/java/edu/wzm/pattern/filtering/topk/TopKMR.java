package edu.wzm.pattern.filtering.topk;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

/**
 * Created by Administrator on 2016/4/10.
 */
public class TopKMR {

    public static class TopKMapper extends Mapper<Object, Text, NullWritable, Text>{

        // Stores a map of user reputation to the record
        private TreeMap<Integer, Text> topMap = new TreeMap<Integer, Text>();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] line = value.toString().split(",");
            Integer id = Integer.parseInt(line[0]);
            String reputation = line[1];
            topMap.put(id, new Text(line[1] + ":" + reputation));

            // If we have more than ten records, remove the one with the lowest rep
            // As this tree map is sorted in descending order, the user with
            // the lowest reputation is the last key.
            if(topMap.size() > 10){
                topMap.remove(topMap.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            // Output our ten records to the reducers with a null key
            for(Text value : topMap.values()) {
                context.write(NullWritable.get(), value);
            }
        }
    }

    public static class TopKReducer extends Reducer<NullWritable, Text, NullWritable, Text>{

        // Stores a map of user reputation to the record
        // Overloads the comparator to order the reputations in descending order
        private TreeMap<Integer, Text> topMap = new TreeMap<Integer, Text>();

        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for(Text value : values){
                String[] val = value.toString().split(":");
                Integer id = Integer.parseInt(val[0]);

                topMap.put(id, new Text(val[1]));

                // If we have more than ten records, remove the one with the lowest rep
                // As this tree map is sorted in descending order, the user with
                // the lowest reputation is the last key.
                if(topMap.size() > 10){
                    topMap.remove(topMap.firstKey());
                }
            }

            for(Text v : topMap.descendingMap().values()){
                context.write(NullWritable.get(), v);
            }
        }
    }
}
