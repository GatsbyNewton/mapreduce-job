package edu.wzm.pattern.organization.partition;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by Administrator on 2016/6/1.
 */
public class PartitionPartitioner extends Partitioner<IntWritable, Text> implements Configurable{

    private static final String MIN_LAST_ACCESS_DATE_YEAR = "min.last.access.date.year";

    private Configuration conf = null;
    private int minAccessYear = 0;

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
        this.minAccessYear = conf.getInt("MIN_LAST_ACCESS_DATE_YEAR", 0);
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public int getPartition(IntWritable key, Text value, int numPartitions) {
        return key.get() - minAccessYear;
    }

    /**
     * Sets the minimum possible last access date to subtract from each key
     * to be partitioned<br>
     * <br>
     *
     * That is, if the last min access date is "2008" and the key to
     * partition is "2009", it will go to partition 2009 - 2008 = 1
     *
     * @param job
     *            The job to configure
     * @param minLastAccessYear
     *            The minimum access date.
     */
    public static void setMinLastAccessDate(Job job, int minLastAccessYear) {
        job.getConfiguration().setInt(MIN_LAST_ACCESS_DATE_YEAR, minLastAccessYear);
    }
}
