package com.zheng.hadoop.mapreduce.wordcount;

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
 * 最后提交到hadoop集群中运行命令：
 * hadoop jar jarpackage.jar com.zheng.hadoop.mapreduce.wordcount.WordCountDriver /wordcount/input /wordcount/output
 * 执行自定义job
 * Created by zhenglian on 2017/11/3.
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        //是否运行为本地模式，就是看这个参数值是否为local，默认就是local
		/*conf.set("mapreduce.framework.name", "local");*/

        //本地模式运行mr程序时，输入输出的数据可以在本地，也可以在hdfs上
        //到底在哪里，就看以下两行配置你用哪行，默认就是file:///
		/*conf.set("fs.defaultFS", "hdfs://mini1:9000/");*/
		/*conf.set("fs.defaultFS", "file:///");*/
        
        //运行集群模式，就是把程序提交到yarn中去运行
        //要想运行为集群模式，以下3个参数要指定为集群上的值
		/*conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.hostname", "mini1");
		conf.set("fs.defaultFS", "hdfs://mini1:9000/");*/
		
        Job job = Job.getInstance(conf);
        
        // 如果要在idea中运行yarn集群，那么必须使用setjar告知具体的jar，不能使用setJarByClass，否则集群根本找不到jar
        job.setJar("C:\\Users\\Administrator\\Desktop\\wordcount.jar");
        
        // 设置job运行的jar在哪里,这里表示与类路径相关，也就是任何地方都可以
//        job.setJarByClass(WordCountDriver.class);

        // 指定本业务job需要使用到的mapper与reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设定mapper输出结果类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置最终输出结果类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // 处理小文件切片印象效率问题，将多个小文件在逻辑上合并在同一个切片中
        // 如果不设置InputFormat，它默认用的是TextInputformat.class
//        job.setInputFormatClass(CombineTextInputFormat.class);
//        CombineFileInputFormat.setMaxInputSplitSize(job, 4194304); // 最大设置为4M
//        CombineFileInputFormat.setMinInputSplitSize(job, 2097152); // 最小设置为2M
        

        // 指定job输入的原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 指定最终输出结果文件所在目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 将job中配置的相关参数以及job所用的jar提交到yarn运行,verbose=true表示打印出日志信息
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

}
