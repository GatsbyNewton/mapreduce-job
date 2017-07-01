package edu.wzm.pattern.join.composite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Administrator on 2016/6/19.
 */
public class CompositeDriver extends Configured implements Tool {
    @Override
    public int run(String[] otherArgs) throws Exception {
        Configuration conf = getConf();
        Path userPath = new Path(otherArgs[0]);
        Path commentPath = new Path(otherArgs[1]);
        Path outputDir = new Path(otherArgs[2]);
        String joinType = otherArgs[3];

        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
        // The composite input format join expression will set how the records
        // are going to be read in, and in what input format.
        conf.set("mapreduce.join.expr", CompositeInputFormat.compose(joinType,
                KeyValueTextInputFormat.class, userPath, commentPath));

        Job job = Job.getInstance(conf, "CompositeJoin");
        job.setJarByClass(CompositeDriver.class);
        job.setMapperClass(CompositeMapper.class);

        job.setNumReduceTasks(0);
        // Set the input format class to a CompositeInputFormat class.
        // The CompositeInputFormat will parse all of our input files and output
        // records to our mapper.

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(CompositeInputFormat.class);
        TextOutputFormat.setOutputPath(job, outputDir);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args)throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 4){
            System.err.println("Usage: CompositeJoin <user data> <comment data> <out> [inner|outer]");
            System.exit(2);
        }

        System.exit(ToolRunner.run(conf, new CompositeDriver(), otherArgs));
    }
}
