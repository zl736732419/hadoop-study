package com.zheng.hadoop.mapreduce.federation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * @Author zhenglian
 * @Date 2017/12/27 18:45
 */
public class FederationAppTest {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bi"), conf, "hadoop");

        fs.copyFromLocalFile(new Path("C:\\Users\\Administrator\\Desktop\\federation\\yarn-site.xml"), 
                new Path("/federation/yarn-site.xml"));
        fs.close();
    }
}
