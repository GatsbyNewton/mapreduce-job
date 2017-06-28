package edu.wzm.multiout;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Administrator on 2016/4/24.
 */
public class Driver extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        conf.set("type", strings[2]);

        Job job = Job.getInstance(conf, "Multiple Output");
        job.setJarByClass(Driver.class);
        job.setMapperClass(MultiOutMR.MultiOutMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(MultiOutMR.MultiOutReducer.class);

        if(!strings[2].equalsIgnoreCase("baseOutputPath")){
            MultipleOutputs.addNamedOutput(job, "hello", TextOutputFormat.class,
                    Text.class, LongWritable.class);
            MultipleOutputs.addNamedOutput(job, "IT", TextOutputFormat.class,
                    Text.class, LongWritable.class);
        }

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args)throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 3){
            System.err.println("Usage: <input> <input> <output type>");
            System.out.println("Type:\n" +
                    "namedOutput - the named output name.\n" +
                    "baseOutputPath - base-output path to write the record to. Note: Framework will generate unique filename for the baseOutputPath.\n" +
                    "all - contains namedOutput and baseOutputPath.");
            System.exit(1);
        }

        System.exit(ToolRunner.run(conf, new Driver(), otherArgs));
    }
}
