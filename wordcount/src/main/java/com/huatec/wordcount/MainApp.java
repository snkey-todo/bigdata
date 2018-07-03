package com.huatec.wordcount;

import org.apache.camel.main.Main;
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
 * A Camel Application
 */
public class MainApp {

    /**
     * A main() so we can easily run these routing rules in our IDE
     */
    public static void main(String... args) throws Exception {
        Job job = Job.getInstance(new Configuration());

        //main方法所在的类
        job.setJarByClass(MainApp.class);

        //配置mapper
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        FileInputFormat.setInputPaths(job, new Path("/sample/sample-wordcount.txt"));

        //配置reducer
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileOutputFormat.setOutputPath(job, new Path("/sample/sample-wordcountresult"));

        //提交job.true:打印进度和详情
        job.waitForCompletion(true);
    }
    class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {
            //接收数据
            String line = value.toString();
            //切分数据,按空格拆分单词。
            String[] words = line.split(" ");
            //循环
            for(String word :words){
                //输出。单词出现一次，计数1
                context.write(new Text(word), new LongWritable(1));
            }
        }
    }

    class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> value,
                              Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            //接收数据
            long counter = 0;
            //循环
            for(LongWritable i: value){ //value是一个迭代器，不能直接获取长度，只能循环计数
                counter += i.get();
            }
            //输出
            context.write(key,new LongWritable(counter));


        }
    }
}

