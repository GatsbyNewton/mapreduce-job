package edu.wzm.pattern.summarization.counter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

/**
 * Created by Administrator on 2016/4/5.
 */
public class CounterMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {

    public static final String STATE_COUNTER_GROUP = "State";
    public static final String UNKNOW_COUNTER = "Unknow";
    public static final String NULL_OR_EMPTY_COUNTER = "Null or empty";

    private String[] statesArray = new String[] { "AL", "AK", "AZ", "AR",
            "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN",
            "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
            "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND",
            "OH", "OK", "OR", "PA", "RI", "SC", "SF", "TN", "TX", "UT",
            "VT", "VA", "WA", "WV", "WI", "WY" };

    private HashSet<String> states = new HashSet<String>(Arrays.asList(statesArray));

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().toUpperCase().split("\\s");

        boolean unknow = true;
        // Look for a state abbreviation code if the
        // location is not null or empty
        if(line[1] !=  null && line[1].isEmpty() && "".equals(line[1])){
            String[] locations = line[1].split("|");
            for(String location : locations){

                // Check if it is a state
                if(states.contains(location)){
                    // If so, increment the state's counter by 1
                    // and flag it as not unknown
                    context.getCounter(STATE_COUNTER_GROUP, location).increment(1);
                    unknow = false;
                    break;
                }
            }

            // If the state is unknown, increment the UNKNOWN_COUNTER counter
            if (unknow) {
                context.getCounter(STATE_COUNTER_GROUP, UNKNOW_COUNTER).increment(1);
            }
        }
        else {
            // If it is empty or null, increment the
            // NULL_OR_EMPTY_COUNTER counter by 1
            context.getCounter(STATE_COUNTER_GROUP, NULL_OR_EMPTY_COUNTER).increment(1);
        }
    }
}
