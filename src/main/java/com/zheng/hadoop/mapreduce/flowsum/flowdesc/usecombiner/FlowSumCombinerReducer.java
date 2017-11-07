package com.zheng.hadoop.mapreduce.flowsum.flowdesc.usecombiner;


import com.zheng.hadoop.mapreduce.flowsum.FlowBean;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 统计用户上下行流量
 * Created by zhenglian on 2017/11/5.
 */
public class FlowSumCombinerReducer extends Reducer<FlowBeanKey, FlowBean, FlowBeanKey, FlowBean>{
    @Override
    protected void reduce(FlowBeanKey key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sumUpFlow = 0L;
        long sumDownFlow = 0L;
        for (FlowBean value : values) {
            sumUpFlow += value.getUpFlow();
            sumDownFlow += value.getDownFlow();
        }
        
        FlowBean flowBean = new FlowBean(sumUpFlow, sumDownFlow);
        key.setSumFlow(flowBean.getSumFlow());
        context.write(key, flowBean);
    }
}
