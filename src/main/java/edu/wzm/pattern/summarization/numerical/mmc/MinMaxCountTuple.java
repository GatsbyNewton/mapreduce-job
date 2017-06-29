package edu.wzm.pattern.summarization.numerical.mmc;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Administrator on 2016/3/31.
 */
public class MinMaxCountTuple implements Writable{

    private Date min = new Date();
    private Date max = new Date();
    private long count = 0L;

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(min.getTime());
        dataOutput.writeLong(max.getTime());
        dataOutput.writeLong(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.min = new Date(dataInput.readLong());
        this.max = new Date(dataInput.readLong());
        this.count = dataInput.readLong();
    }

    public Date getMin() {
        return min;
    }

    public void setMin(Date min) {
        this.min = min;
    }

    public Date getMax() {
        return max;
    }

    public void setMax(Date max) {
        this.max = max;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return sdf.format(max) + "\t" + sdf.format(min) + "\t" + count;
    }
}
