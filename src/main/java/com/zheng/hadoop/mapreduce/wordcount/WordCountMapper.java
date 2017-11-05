package com.zheng.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 统计单词，map
 * mr读到的一行内容进行map操作
 * 
 * KEYIN 默认情况下，是mr框架所读到的一行的起始偏移量，类型Long
 * 但是在hadoop中已经提供了更加精简的序列化接口，所以不是直接使用Long，而是LongWritable
 * VALUEIN 默认情况下，是mr框架读到的一行内容 String,这里用Text
 * KEYOUT 是用户自定义逻辑处理完成后输出的数据中的key,这里是单词，所以使用Text
 * VALUEOUT 是用户自定义逻辑处理完成后输出的数据中的value,这里是单词的总数，所以使用IntWritable
 * 
 * Created by zhenglian on 2017/11/2.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * map阶段的业务逻辑就写在map方法中
     * maptask会对每一行读到的数据调用map方法
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // 这里只是简单的做分词，只考虑英文单词
        String[] words = line.split(" ");
        for(String word : words) {
            // 这里将单词作为key, 将次数作为1，以便于后续的数据分发，可以根据单词进行分发，将相同单词分发到相同的reduce
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
