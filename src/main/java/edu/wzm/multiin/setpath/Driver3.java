package edu.wzm.multiin.setpath;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver3 extends Configured implements Tool{

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Add Path");
        job.setJarByClass(Driver3.class);
        job.setMapperClass(SetPathMR.SetPathMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(SetPathMR.SetPathReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        String paths = strings[0] + "," + strings[1];
        FileInputFormat.setInputPaths(job, paths);
        FileOutputFormat.setOutputPath(job, new Path(strings[2]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args)throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 3){
            System.err.println("Usage: <input1> <input2> <output>");
            System.exit(1);
        }

        System.exit(ToolRunner.run(conf, new Driver3(), otherArgs));
    }
}