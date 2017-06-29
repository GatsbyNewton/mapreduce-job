package edu.wzm.pattern.summarization.numerical.mmc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Administrator on 2016/4/2.
 */
public class MinMaxCountDriver extends Configured implements Tool{

    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = getConf();
        Job job = new Job(conf, "Min Max Count");
        job.setJarByClass(MinMaxCountDriver.class);
        job.setMapperClass(MinMaxCountMR.MinMaxCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MinMaxCountTuple.class);
        job.setReducerClass(MinMaxCountMR.MinMaxCountReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args)throws  Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 2){
            System.err.println("Usage: <input> <output>");
            System.exit(2);
        }

        System.exit(ToolRunner.run(conf, new MinMaxCountDriver(), otherArgs));
    }
}
