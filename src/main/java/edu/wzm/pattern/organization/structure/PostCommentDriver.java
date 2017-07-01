package edu.wzm.pattern.organization.structure;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Administrator on 2016/4/23.
 */
public class PostCommentDriver extends Configured implements Tool{
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf, "PostCommentHierarchy");
        job.setJarByClass(PostCommentDriver.class);

        MultipleInputs.addInputPath(job, new Path(strings[0]), TextInputFormat.class,
                PostCommentMR.PostMapper.class);
        MultipleInputs.addInputPath(job, new Path(strings[1]), TextInputFormat.class,
                PostCommentMR.CommentMapper.class);
        job.setReducerClass(PostCommentMR.PostCommentReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(strings[2]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args)throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 3){
            System.err.println("Usage: <input post> <input comment> <output>");
            System.exit(2);
        }

        System.exit(ToolRunner.run(conf, new PostCommentDriver(), otherArgs));
    }
}
