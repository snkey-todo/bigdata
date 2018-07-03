package com.huatec.phonedata;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by zhusheng on 2017/11/22.
 */
public class DCReducer extends Reducer<Text, DataBean, Text, DataBean> {

    @Override
    protected void reduce(Text key, Iterable<DataBean> values, Context context)
            throws IOException, InterruptedException {
        // //定义2个变量用于统计
        long up_sum = 0;
        long down_sum = 0;
        for (DataBean bean : values) {
            up_sum += bean.getUpPayLoad();
            down_sum += bean.getDownPayLoad();
        }
        DataBean bean = new DataBean("", up_sum, down_sum);
        context.write(key, bean);
    }
}
