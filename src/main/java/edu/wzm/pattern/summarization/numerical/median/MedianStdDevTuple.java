package edu.wzm.pattern.summarization.numerical.median;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 2016/4/4.
 */
public class MedianStdDevTuple implements Writable {

    private double median = 0;
    private double stdDev = 0;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(median);
        dataOutput.writeDouble(stdDev);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.median = dataInput.readDouble();
        this.stdDev = dataInput.readDouble();
    }

    public double getMedian() {
        return median;
    }

    public void setMedian(double median) {
        this.median = median;
    }

    public double getStdDev() {
        return stdDev;
    }

    public void setStdDev(double stdDev) {
        this.stdDev = stdDev;
    }
}
