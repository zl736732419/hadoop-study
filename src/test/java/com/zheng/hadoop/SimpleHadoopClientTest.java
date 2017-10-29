package com.zheng.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

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
    
    private FileSystem fs = null;

    @Before
    public void init() throws Exception {
        Configuration conf = new Configuration();
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
    
    
}
