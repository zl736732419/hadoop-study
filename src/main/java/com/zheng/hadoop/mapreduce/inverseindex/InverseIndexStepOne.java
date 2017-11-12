package com.zheng.hadoop.mapreduce.inverseindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 第一步 统计每一个单词在对应文件中出现的次数
 * 把单词和文件名称作为key进行mapper
 * Created by zhenglian on 2017/11/12.
 */
public class InverseIndexStepOne {

    static class StepOneMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");

            // 获取文件名
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();

            for (String word : words) {
                k.set(word + " " + filename);
                context.write(k, new IntWritable(1));
            }
        }
    }

    /**
     * 统计每一个文件单词出现的次数
     */
    static class StepOneReducer extends Reducer<Text, IntWritable, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }

            StringBuilder builder = new StringBuilder();
            builder.append("--").append(count);

            context.write(key, new Text(builder.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(InverseIndexStepOne.class);

        job.setMapperClass(StepOneMapper.class);
        job.setReducerClass(StepOneReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path outpath = new Path("C:\\Users\\Administrator\\Desktop\\hadooptest\\inverseindex\\output");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outpath)) {
            fs.delete(outpath, true);
        }
        
        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\Administrator\\Desktop\\hadooptest\\inverseindex\\input"));
        FileOutputFormat.setOutputPath(job, outpath);
        
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
