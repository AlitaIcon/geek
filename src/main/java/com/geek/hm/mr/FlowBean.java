package com.geek.hm.mr;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class FlowBean implements Writable {
    Integer upFlow;
    Integer downFlow;
    Integer countFlow;


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(upFlow);
        out.writeInt(downFlow);
        out.writeInt(countFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readInt();
        this.downFlow = in.readInt();
        this.countFlow = in.readInt();
    }

    @Override
    public String toString() {
        return upFlow + " " + downFlow + " " + countFlow;
    }

    public Integer getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Integer upFlow) {
        this.upFlow = upFlow;
    }

    public Integer getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Integer downFlow) {
        this.downFlow = downFlow;
    }

    public Integer getCountFlow() {
        return countFlow;
    }

    public void setCountFlow(Integer countFlow) {
        this.countFlow = countFlow;
    }

}
