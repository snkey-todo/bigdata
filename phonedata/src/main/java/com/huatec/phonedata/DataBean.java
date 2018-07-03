package com.huatec.phonedata;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zhusheng on 2017/11/20.
 */
public class DataBean implements Writable {
    private String tel; //手机号码

    private long upPayLoad; //上行流量

    private long downPayLoad;   //下行流量

    private long totalPayLoad;  //总流量


    public DataBean(){}

    public DataBean(String tel, long upPayLoad, long downPayLoad) {
        super();
        this.tel = tel;
        this.upPayLoad = upPayLoad;
        this.downPayLoad = downPayLoad;
        this.totalPayLoad = upPayLoad + downPayLoad;
    }


    @Override
    public String toString() {
        return this.upPayLoad + "\t" + this.downPayLoad + "\t" + this.totalPayLoad;
    }

   //序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(tel);
        out.writeLong(upPayLoad);
        out.writeLong(downPayLoad);
        out.writeLong(totalPayLoad);
    }

    //反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        this.tel = in.readUTF();
        this.upPayLoad = in.readLong();
        this.downPayLoad = in.readLong();
        this.totalPayLoad = in.readLong();

    }

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public long getUpPayLoad() {
        return upPayLoad;
    }

    public void setUpPayLoad(long upPayLoad) {
        this.upPayLoad = upPayLoad;
    }

    public long getDownPayLoad() {
        return downPayLoad;
    }

    public void setDownPayLoad(long downPayLoad) {
        this.downPayLoad = downPayLoad;
    }

    public long getTotalPayLoad() {
        return totalPayLoad;
    }

    public void setTotalPayLoad(long totalPayLoad) {
        this.totalPayLoad = totalPayLoad;
    }
}
