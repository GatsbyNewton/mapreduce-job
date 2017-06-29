package edu.wzm.pattern.join.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Administrator on 2016/6/16.
 */
public class ReducerSideDriver extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "ReducerSideJoin");
        job.setJarByClass(ReducerSideDriver.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(strings[0]), TextInputFormat.class,
                ReducerSideMR.UserMapper.class);
        MultipleInputs.addInputPath(job, new Path(strings[1]), TextInputFormat.class,
                ReducerSideMR.CommentMapper.class);

        job.setReducerClass(ReducerSideMR.ReducerSideReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(strings[2]));

        return job.waitForCompletion(true) ? 0: 1;
    }

    public static void main(String[] args)throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 4){
            System.err.println("Usage: <user input> <comment input> <output> <join type>");
            System.exit(2);
        }

        String joinType = otherArgs[3];
        if (!(joinType.equalsIgnoreCase("inner")
                || joinType.equalsIgnoreCase("left")
                || joinType.equalsIgnoreCase("right")
                || joinType.equalsIgnoreCase("full")
                || joinType.equalsIgnoreCase("anti"))) {
            System.err.println("Join type not set to inner, leftouter, rightouter, fullouter, or anti");
            System.exit(2);
        }

        ToolRunner.run(conf, new ReducerSideDriver(), otherArgs);
    }
}
