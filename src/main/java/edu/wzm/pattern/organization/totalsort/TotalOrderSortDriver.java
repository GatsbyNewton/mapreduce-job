package edu.wzm.pattern.organization.totalsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Administrator on 2016/6/11.
 */
public class TotalOrderSortDriver extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Path input = new Path(strings[0]);
        Path partitionFile = new Path(strings[1] + "_partitions.lst");
        Path outputStage = new Path(strings[1] + "_staging");
        Path outputOrder = new Path(strings[1]);
        double sampleRate = Double.parseDouble(strings[2]);

        Configuration conf = getConf();
        Job sampleJob = Job.getInstance(conf, "TotalOrderSortingStage");
        sampleJob.setJarByClass(TotalOrderSortDriver.class);
        sampleJob.setMapperClass(FirstPhaseMapper.class);
        sampleJob.setMapOutputKeyClass(LongWritable.class);
        sampleJob.setMapOutputValueClass(Text.class);

        sampleJob.setNumReduceTasks(0);

        FileInputFormat.addInputPath(sampleJob, input);
        // Set the output format to a sequence file
        sampleJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(sampleJob, outputStage);

        int statusCode = sampleJob.waitForCompletion(true) ? 0 : 1;

        if(statusCode == 0){
            Job orderJob = Job.getInstance(conf, "TotalOrderSortingStage");
            // Here, use the identity mapper to output the key/value pairs in
            // the SequenceFile
            orderJob.setMapperClass(Mapper.class);

            orderJob.setReducerClass(SecondPhaseReducer.class);
            // Set the number of reduce tasks to an appropriate number for the
            // amount of data being sorted
            orderJob.setNumReduceTasks(5);

            // Use Hadoop's TotalOrderPartitioner class
            orderJob.setPartitionerClass(TotalOrderPartitioner.class);
            // Set the partition file
            TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
            orderJob.setOutputKeyClass(LongWritable.class);
            orderJob.setOutputValueClass(Text.class);

            // Set the input to the previous job's output
            orderJob.setInputFormatClass(SequenceFileInputFormat.class);
            SequenceFileInputFormat.setInputPaths(orderJob, outputStage);

            TextOutputFormat.setOutputPath(orderJob, outputOrder);
            // Set the separator to an empty string
            orderJob.getConfiguration().set("mapreduce.output.textoutputformat.separator", "");

            // Use the InputSampler to go through the output of the previous
            // job, sample it, and create the partition file
            InputSampler.writePartitionFile(orderJob,
                    new InputSampler.RandomSampler(sampleRate, 100));

            // Submit the job
            statusCode = orderJob.waitForCompletion(true) ? 0 : 2;
        }

        if(statusCode == 0) {
            FileSystem hdfs = FileSystem.get(conf);

            hdfs.delete(partitionFile, true);
            hdfs.delete(outputStage, true);
            return statusCode;
        }
        else {
            return statusCode;
        }
    }

    public static void main(String[] args)throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 3){
            System.err.println("Usage: <input> <output> <sample rate>");
            System.exit(2);
        }

        System.exit(ToolRunner.run(conf, new TotalOrderSortDriver(), otherArgs));
    }
}
