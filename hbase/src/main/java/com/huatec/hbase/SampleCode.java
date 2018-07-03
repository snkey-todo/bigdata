package com.huatec.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by zhusheng on 2017/12/14.
 */
public class SampleCode {
    private static Configuration conf = null;

    static {
        conf = HBaseConfiguration.create();
        //生产环境
        conf.set("hbase.zookeeper.quorum", "huatec05:2181,huatec06:2181,huatec07:2181");
    }
    public static void main(String[] args) throws Exception {

        //sample
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.enableTable("people");


        //sample
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        hBaseConfiguration.set("hbase.zookeeper.quorum", "huatec05:2181,huatec06:2181,huatec07:2181");

        //sample
        HTableDescriptor htd = new HTableDescriptor("poeple");
        htd.addFamily(new HColumnDescriptor("info"));
        htd.setValue("age","20");
        htd.setValue("name", "Smith");

        //sample
        HColumnDescriptor hcd = new HColumnDescriptor("content:");
        htd.addFamily(hcd);


        //sample
        HTable hTable = new HTable(conf, Bytes.toBytes("people"));
        Get get = new Get(Bytes.toBytes("rk49999"));
        Result result =  hTable.get(get);
    }
}
