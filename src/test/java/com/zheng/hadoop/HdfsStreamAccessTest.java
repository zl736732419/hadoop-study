package com.zheng.hadoop;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.PrintStream;
import java.net.URI;


/**
 * Created by zhenglian on 2017/10/30.
 */
public class HdfsStreamAccessTest {
    
    private FileSystem fs;
    
    @Before
    public void init() throws Exception {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "3");
        fs = FileSystem.get(new URI("hdfs://192.168.1.200:9000"), conf, "hadoop");
    }

    /**
     * 文件上传
     * @throws Exception
     */
    @Test
    public void upload() throws Exception {
        FSDataOutputStream output = fs.create(new Path("/qingqing.love2"), true);
        FileInputStream input = new FileInputStream("d:/qingqing.love");
        IOUtils.copy(input, output);
    }
    
    @Test
    public void download() throws Exception {
        FSDataInputStream input = fs.open(new Path("/qingqing.love"));
        PrintStream output = System.out;
        IOUtils.copy(input, output);
    }

    @Test
    public void downloadSeek() throws Exception {
        FSDataInputStream input = fs.open(new Path("/qingqing.love"));
        input.seek(12); // 从12字节开始读取
        PrintStream output = System.out;
        IOUtils.copy(input, output);
    }
    
}
