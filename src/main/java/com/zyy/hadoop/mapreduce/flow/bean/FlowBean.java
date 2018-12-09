package com.zyy.hadoop.mapreduce.flow.bean;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 2018/10/2.
 */
public class FlowBean implements WritableComparable<FlowBean> {
    private long phone;
    private long upStream;
    private long downStream;
    private String name;

    //反射需要用到，必须显示声明
    public FlowBean() {
    }

    public FlowBean(long phone, long upStream, long downStream, String name) {
        this.phone = phone;
        this.upStream = upStream;
        this.downStream = downStream;
        this.name = name;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeLong(phone);
        out.writeLong(upStream);
        out.writeLong(downStream);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.name = in.readUTF();
        this.phone = in.readLong();
        this.upStream = in.readLong();
        this.downStream = in.readLong();
    }

    @Override
    public String toString() {
        return phone + " " + upStream + " " + downStream + " " + name;
    }

    public long getPhone() {
        return phone;
    }

    public void setPhone(long phone) {
        this.phone = phone;
    }

    public long getUpStream() {
        return upStream;
    }

    public void setUpStream(long upStream) {
        this.upStream = upStream;
    }

    public long getDownStream() {
        return downStream;
    }

    public void setDownStream(long downStream) {
        this.downStream = downStream;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


    @Override
    public int compareTo(FlowBean o) {
        if (this.phone >= o.getPhone()) {
            return -23;
        } else if (this.phone < o.getPhone()) {
            return 23;
        }
        return 0;
    }
}
