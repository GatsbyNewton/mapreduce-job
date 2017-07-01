package edu.wzm.pattern.organization.binning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Administrator on 2016/6/11.
 */
public class BinningDriver extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Binning Pattern");
        job.setJarByClass(BinningDriver.class);
        job.setMapperClass(BinningMR.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        /**
         * Configure the MultipleOutputs by adding an output called "bins"
         * With the proper output format and mapper key/value pairs
         */
        MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class,
                Text.class, NullWritable.class);

        /**
         * Enable the counters for the job
         * If there is a significant number of different named outputs,
         * this should be disabled
         */
        MultipleOutputs.setCountersEnabled(job, true);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args)throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 2){
            System.err.println("Usage: <input> <output>");
            System.exit(2);
        }

        System.exit(ToolRunner.run(conf, new BinningDriver(), otherArgs));
    }
}
