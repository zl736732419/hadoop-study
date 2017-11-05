package com.zheng.hadoop.mapreduce.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 统计用户上行流量、下行流量、总流量
 * 输出 用户手机号 上行流量 下行流量 总流量
 * Created by zhenglian on 2017/11/5.
 */
public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 转成一行
        String line = value.toString();
        // 拆分
        String[] arrs = line.split("\t");
        
        // 获取手机号
        String mobile = arrs[1];
        
        // 获取上下行流量
        long upFlow = Long.parseLong(arrs[arrs.length - 3]);
        long downFlow = Long.parseLong(arrs[arrs.length - 2]);
        
        FlowBean flowBean = new FlowBean(upFlow, downFlow);
        context.write(new Text(mobile), flowBean);
    }
}
