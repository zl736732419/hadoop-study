package com.zheng.hadoop.mapreduce.flowsum.flowdesc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 统计用户上行流量、下行流量、总流量
 * 并按照总流量逆序，这里的mr程序需要建立在之前流量统计的结果文件上
 * Created by zhenglian on 2017/11/5.
 */
public class FlowDescSumMapper extends Mapper<LongWritable, Text, FlowDescBean, Text>{

    private FlowDescBean flowDescBean = new FlowDescBean();
    private Text v = new Text();
    
    /**
     * 流量统计结果：
     * 手机号        上行      下行     总流量
     13502468823	7335	110349	117684
     13925057413	11058	48243	59301
     13726238888	4962	49362	54324
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] arrs = line.split("\t");
        String mobile = arrs[0];
        Long upFlow = Long.parseLong(arrs[1]);
        Long downFlow = Long.parseLong(arrs[2]);
        
        flowDescBean.set(upFlow, downFlow);
        v.set(mobile);
        context.write(flowDescBean, v);

    }
}
