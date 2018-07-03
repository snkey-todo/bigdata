package com.huatec.dc2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by zhusheng on 2017/12/29.
 */
public class DataCount {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(DataCount.class);

        job.setMapperClass(DCMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DataBean.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        job.setReducerClass(DCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DataBean.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

    class DCMapper extends Mapper<LongWritable, Text, Text, DataBean> {
        @Override
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, Text, DataBean>.Context context)
                throws IOException, InterruptedException {

            //取得自己想要的数据
            String line = value.toString();
            String[] fields = line.split("\t");
            String phoneNumber = fields[1];
            long upPayload = Long.parseLong(fields[8]);
            long downPayload = Long.parseLong(fields[9]);

            //输出
            context.write(new Text(phoneNumber), new DataBean(phoneNumber, upPayload, downPayload, upPayload + downPayload));
        }
    }

    class DCReducer extends Reducer<Text, DataBean, Text, DataBean> {
        @Override
        protected void reduce(Text key, Iterable<DataBean> value,
                              Reducer<Text, DataBean, Text, DataBean>.Context context)
                throws IOException, InterruptedException {
            //统计流量
            long upPaySum = 0;
            long downPaySum = 0;
            for (DataBean bean : value) {
                upPaySum += bean.getUpPayLoad();
                downPaySum += bean.getDownPayLoad();
            }
            //输出
            context.write(key, new DataBean(key.toString(), upPaySum, downPaySum, upPaySum + downPaySum));
        }
    }
}
