package com.zheng.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 单词统计 reduce
 * KEYIN VALUEIN 对应于mapper的输出参数
 * KEYOUT 这里是最后统计的单词
 * VALUEOUT 这里是最后统计的单词出现总次数
 * Created by zhenglian on 2017/11/2.
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) { // 将相同单词的出现次数进行合并汇总
            count += value.get();
        }
        
        // 将最后统计的结果输出
        context.write(key, new IntWritable(count));
    }
}
