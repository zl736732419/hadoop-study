package com.zheng.hadoop.mapreduce.flowsum.flowdesc;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 统计用户上下行流量
 * Created by zhenglian on 2017/11/5.
 */
public class FlowDescSumReducer extends Reducer<FlowDescBean, Text, Text, FlowDescBean>{
    private Text key = new Text();
    @Override
    protected void reduce(FlowDescBean flowDescBean, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String mobile = values.iterator().next().toString();
        key.set(mobile);
        context.write(key, flowDescBean);
    }
}
