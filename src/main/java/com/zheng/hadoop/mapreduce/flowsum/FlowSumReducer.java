package com.zheng.hadoop.mapreduce.flowsum;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 统计用户上下行流量
 * Created by zhenglian on 2017/11/5.
 */
public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sumUpFlow = 0L;
        long sumDownFlow = 0L;
        for (FlowBean value : values) {
            sumUpFlow += value.getUpFlow();
            sumDownFlow = value.getDownFlow();
        }
        
        FlowBean flowBean = new FlowBean(sumUpFlow, sumDownFlow);
        context.write(key, flowBean);
    }
}
