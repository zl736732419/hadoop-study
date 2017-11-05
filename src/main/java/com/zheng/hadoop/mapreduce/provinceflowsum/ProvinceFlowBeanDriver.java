package com.zheng.hadoop.mapreduce.provinceflowsum;

import com.zheng.hadoop.mapreduce.flowsum.FlowBean;
import com.zheng.hadoop.mapreduce.flowsum.FlowSumMapper;
import com.zheng.hadoop.mapreduce.flowsum.FlowSumReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by zhenglian on 2017/11/5.
 */
public class ProvinceFlowBeanDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(ProvinceFlowBeanDriver.class);

        job.setMapperClass(FlowSumMapper.class);
        job.setReducerClass(FlowSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 设置reducetask任务个数，与分区一致，与业务逻辑中划分的区域个数一致
        job.setNumReduceTasks(5);
        job.setPartitionerClass(ProvincePartitioner.class);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

}
