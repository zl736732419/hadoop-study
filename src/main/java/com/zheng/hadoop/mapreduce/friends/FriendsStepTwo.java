package com.zheng.hadoop.mapreduce.friends;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 配对<人，人>
 * Created by zhenglian on 2017/11/12.
 */
public class FriendsStepTwo {
    
    static class FriendMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text k = new Text();
        private Text v = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            // A	I, K, C, B, G, F, H, O, D
            String[] arrs = line.split("\t");
            String friend = arrs[0];
            v.set(friend);
            String[] persons = arrs[1].split(",");
            // 排序人，人  b-c  c-b 是一样的效果
            Arrays.sort(persons);
            for (int i = 0; i < persons.length - 1; i++) {
                for (int j = i + 1; j < persons.length; j++) {
                    k.set(persons[i] + "-" + persons[j]);
                    context.write(k, v);
                }
            }
        }
    }
    
    static class FriendReducer extends Reducer<Text, Text, Text, Text> {

        private Text v = new Text();
        
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<String> friends = new ArrayList<>();
            for (Text value : values) {
                friends.add(value.toString());
            }
            
            String friendsStr = StringUtils.join(friends);
            friendsStr = friendsStr.substring(1, friendsStr.length() - 1);
            v.set(friendsStr);
            context.write(key, v);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(FriendsStepTwo.class);

        job.setMapperClass(FriendsStepTwo.FriendMapper.class);
        job.setReducerClass(FriendsStepTwo.FriendReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\Administrator\\Desktop\\hadooptest\\friends\\output"));
        Path outpath = new Path("C:\\Users\\Administrator\\Desktop\\hadooptest\\friends\\output2");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outpath)) {
            fs.delete(outpath, true);
        }
        FileOutputFormat.setOutputPath(job, outpath);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
    
}
