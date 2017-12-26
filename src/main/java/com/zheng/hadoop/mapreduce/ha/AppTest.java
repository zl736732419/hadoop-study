package com.zheng.hadoop.mapreduce.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * Created by zhenglian on 2017/11/18.
 */
public class AppTest {
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bi"), conf, "hadoop");
        
        fs.copyFromLocalFile(new Path("C:\\Users\\Administrator\\Desktop\\wordcount.jar"), new Path("/wordcount.jar"));
        fs.close();
    }
    
    
}
