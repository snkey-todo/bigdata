package com.huatec.dc2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zhusheng on 2017/12/29.
 */
public class DataBean implements Writable {
    private String phoneNumber;//手机号码
    private long upPayLoad;//上行流量
    private long downPayLoad;//下行流量
    private long totalPayLoad;//总流量

    public DataBean() {
        super();
    }

    public DataBean(String phoneNUmber, long upPayLoad, long downPayLoad, long totalPayLoad) {
        super();
        this.phoneNumber = phoneNUmber;
        this.upPayLoad = upPayLoad;
        this.downPayLoad = downPayLoad;
        this.totalPayLoad = totalPayLoad;
    }

    public String getPhoneNUmber() {
        return phoneNumber;
    }

    public void setPhoneNUmber(String phoneNUmber) {
        this.phoneNumber = phoneNUmber;
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
    @Override
    public String toString() {
        return upPayLoad +"\t"+downPayLoad + "\t"+ totalPayLoad;
    }
    //反序列化：注意顺序
    public void readFields(DataInput in) throws IOException {
        this.phoneNumber = in.readUTF();
        this.upPayLoad = in.readLong();
        this.downPayLoad = in.readLong();
        this.totalPayLoad = in.readLong();
    }
    //序列化
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phoneNumber);
        out.writeLong(upPayLoad);
        out.writeLong(downPayLoad);
        out.writeLong(totalPayLoad);
    }
}
