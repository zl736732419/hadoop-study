package com.zheng.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.StringUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;

/**
 * 1. 通过编程调用hadoop客户端API跟环境有关，如果是windows版本，那么需要编译hadoop支持windows
 * 2. 客户端操作hdfs时，是有一个身份验证的
 * 默认情况下，hdfs客户端会从jvm获取一个参数作为自己的用户身份：-DHADOOP_USER_NAME=hadoop
 * 也可以在构造客户端的时候指定这个身份
 * 3. 在windows环境下运行下载文件报错，提示
 * org.apache.hadoop.io.nativeio.NativeIO$Windows.createFileWithMode0(Ljava/lang/String;JJJI)Ljava/io/FileDescriptor; 
 * java.lang.UnsatisfiedLinkError
 * 这里主要是检测path路径下是否存在hadoop.dll文件，如果存在则任务hadoop环境是集群环境，但是hadoop不支持windows版集群，所以报错
 * 解决方法就是重命名或者删除hadoop/bin/hadoop.dll
 * Created by zhenglian on 2017/10/29.
 */
public class SimpleHadoopClientTest {
    
    private FileSystem fs;
    private Configuration conf;

    @Before
    public void init() throws Exception {
        conf = new Configuration();
        conf.set("dfs.replication", "2");
        fs = FileSystem.get(new URI("hdfs://192.168.1.200:9000"), conf, "hadoop");
    }
    
    @Test
    public void testUpload() throws Exception {
        fs.copyFromLocalFile(new Path("D:\\resources\\hadoop-2.7.4\\hadoop-2.7.4\\LICENSE.txt"), new Path("/license.txt.copy3"));
        fs.close();
    }
    
    @Test
    public void testDownload() throws Exception {
        fs.copyToLocalFile(false, new Path("/license.txt.copy"), new Path("d:/"), false);
        fs.close();
    }
    
    @Test
    public void testConf() {
        Iterator<Map.Entry<String, String>> it = conf.iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }
    
    @Test
    public void mkdir() throws Exception {
        fs.mkdirs(new Path("/wordcount/input"));
        fs.close();
    }
    
    @Test
    public void delete() throws Exception {
        fs.delete(new Path("/wordcount/input"), true);
        fs.close();
    }
    
    @Test
    public void testLs() throws Exception {
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
        while (files.hasNext()) {
            System.out.println("================================");
            LocatedFileStatus status = files.next();
            System.out.println("blocksize: " + status.getBlockSize());
            System.out.println("accesstime: " + status.getAccessTime());
            System.out.println("owner: " + status.getOwner());
            System.out.println("replication: " + status.getReplication());
            System.out.println("name: " + status.getPath().getName());
            BlockLocation[] locations = status.getBlockLocations();
            for (BlockLocation bl : locations) {
                System.out.println("块偏移量" + bl.getOffset());
                System.out.println("块大小" + bl.getLength());
                System.out.println("名字: " + StringUtils.join(",", bl.getNames()));
                System.out.println("主机: " + StringUtils.join(",", bl.getHosts()));
                
            }
            
        }
    }
    
    @Test
    public void testLs2() throws IOException {
        FileStatus[] statuses = fs.listStatus(new Path("/"));
        for (FileStatus status : statuses) {
            System.out.println("============================");
            System.out.println("name: " + status.getPath().getName());
            System.out.println(status.isDirectory() ? "directory" : "file");
        }
    }
    
    
}
