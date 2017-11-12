package com.zheng.hadoop.mapreduce.friends.eachother;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 输出<人，人> -> num 其中num>2则表示两则两者为互粉
 * Created by zhenglian on 2017/11/13.
 */
public class EachOtherFriendStepOne {
    static class FriendMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text k = new Text();
        private IntWritable v = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            String line = value.toString();
            // A:B,C,D,F,E,O
            String[] arrs = line.split(":");
            String first = arrs[0];
            String[] friends = arrs[1].split(",");
            String friend;
            String keyStr;
            for (int i = 0; i < friends.length; i++) {
                friend = friends[i];
                if (first.compareTo(friend)<0) {
                    keyStr = first + "&" + friend;
                } else {
                    keyStr = friend + "&" + first;
                }
                k.set(keyStr);
                context.write(k, v);
            }
        }
    }
    
    static class FriendReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            if (count > 1) {
                context.write(key, NullWritable.get());
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        
        job.setJarByClass(EachOtherFriendStepOne.class);
        
        job.setMapperClass(FriendMapper.class);
        job.setReducerClass(FriendReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\Administrator\\Desktop\\hadooptest\\friends\\input"));
        Path outpath = new Path("C:\\Users\\Administrator\\Desktop\\hadooptest\\friends\\eachotheroutput");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outpath)) {
            fs.delete(outpath, true);
        }
        FileOutputFormat.setOutputPath(job, outpath);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
        

    }
    
}
