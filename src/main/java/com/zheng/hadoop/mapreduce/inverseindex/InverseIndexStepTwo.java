package com.zheng.hadoop.mapreduce.inverseindex;

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

/**
 * Created by zhenglian on 2017/11/12.
 */
public class InverseIndexStepTwo {
    
    static class StepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {
        
        private Text k = new Text();
        
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            String line = value.toString();
            
            String[] arrs = line.split(" ");
            
            String result = arrs[1].replace("\t", "");
            
            k.set(arrs[0]);
            context.write(k, new Text(result));
        }
    }
    
    static class StepTwoReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            StringBuilder builder = new StringBuilder();
            for (Text value : values) {
                builder.append(value.toString()).append(" ");
            }
            
            context.write(key, new Text(builder.toString()));
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(InverseIndexStepTwo.class);

        job.setMapperClass(StepTwoMapper.class);
        job.setReducerClass(StepTwoReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path outpath = new Path("C:\\Users\\Administrator\\Desktop\\hadooptest\\inverseindex\\output2");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outpath)) {
            fs.delete(outpath, true);
        }

        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\Administrator\\Desktop\\hadooptest\\inverseindex\\output"));
        FileOutputFormat.setOutputPath(job, outpath);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
    
    
}
