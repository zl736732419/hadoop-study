package com.zheng.hadoop.mapreduce.friends.shared;

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
import java.util.List;

/**
 * 求共同好友
 * 1. 求出某人是那些人的好友
 * Created by zhenglian on 2017/11/12.
 */
public class FriendsStepOne {

    static class FriendMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text k = new Text();
        private Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            // A:B,C,D,F,E,O
            String[] arrs = line.split(":");
            String person = arrs[0];
            v.set(person);
            String[] friends = arrs[1].split(",");
            for (String friend : friends) {
                k.set(friend);
                context.write(k, v);
            }
        }
    }

    /**
     * 合并<人,人>之间的好友列表
     */
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
            friendsStr = friendsStr.replace(" ", "");
            v.set(friendsStr);

            context.write(key, v);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(FriendsStepOne.class);

        job.setMapperClass(FriendMapper.class);
        job.setReducerClass(FriendReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\Administrator\\Desktop\\hadooptest\\friends\\input"));
        Path outpath = new Path("C:\\Users\\Administrator\\Desktop\\hadooptest\\friends\\output");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outpath)) {
            fs.delete(outpath, true);
        }
        FileOutputFormat.setOutputPath(job, outpath);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }

}
