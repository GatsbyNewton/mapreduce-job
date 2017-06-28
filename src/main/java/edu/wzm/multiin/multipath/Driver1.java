package edu.wzm.multiin.multipath;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by wangzhimingissac on 2016/3/28.
 */
public class Driver1 extends Configured implements Tool{

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job job =new Job(conf, "Multi Input");
        job.setJarByClass(Driver1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MultiPathMR.MultiReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,
                MultiPathMR.MultiMap1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class,
                MultiPathMR.MultiMap2.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args)throws Exception{

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 3){
            System.err.println("Usage: <input1> <input2> <output>");
            System.exit(1);
        }

        System.exit(ToolRunner.run(conf, new Driver1(), otherArgs));
    }
}
