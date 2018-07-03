package com.huatec.phonedata;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by zhusheng on 2017/11/22.
 */
public class DCMapper extends Mapper<LongWritable, Text, Text, DataBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //accept
        String line = value.toString();
        //split
        String[] fields = line.split("\t");
        String tel = fields[1];
        long up = Long.parseLong(fields[4]);
        long down = Long.parseLong(fields[5]);
        DataBean bean = new DataBean(tel, up, down);
        //send
        context.write(new Text(tel), bean);
    }
}
