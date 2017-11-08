package com.zheng.hadoop.mapreduce.flowsum.flowdesc.usecombiner;

import com.zheng.hadoop.mapreduce.flowsum.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 实验证明，combiner针对这种情形也是不能满足的
 * combiner的运行时机是在map的merge之前或者之后
 * 按照这样的方式的出来的结果时错误的
 * Created by zhenglian on 2017/11/5.
 */
public class FlowBeanDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(FlowBeanDriver.class);

        job.setMapperClass(FlowSumCombinerMapper.class);
        job.setReducerClass(FlowSumCombinerReducer.class);
        job.setMapOutputKeyClass(FlowBeanKey.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(FlowBeanKey.class);
        job.setOutputValueClass(FlowBean.class);

        // 这里事先设置相同的key汇聚在一起
//        job.setCombinerClass(FlowSumCombinerReducer.class);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

}
