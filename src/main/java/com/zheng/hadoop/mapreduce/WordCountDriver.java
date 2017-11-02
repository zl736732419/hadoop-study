package com.zheng.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 任务驱动程序，负责将mapreduce封装成job提交到yarn进行调度运行
 * Created by zhenglian on 2017/11/3.
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        
        // 设置job运行的jar在哪里,这里表示与类路径相关，也就是任何地方都可以
        job.setJarByClass(WordCountDriver.class);

        // 指定本业务job需要使用到的mapper与reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设定mapper输出结果类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置最终输出结果类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 指定job输入的原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 指定最终输出结果文件所在目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 将job中配置的相关参数以及job所用的jar提交到yarn运行,verbose=true表示打印出日志信息
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

}
