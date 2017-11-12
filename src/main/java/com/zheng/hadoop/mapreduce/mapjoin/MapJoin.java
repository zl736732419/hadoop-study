package com.zheng.hadoop.mapreduce.mapjoin;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * 分析订单与产品表之间的关系，如果每一个产品id对应由一个reducer来处理，
 * 那么和可能会有这种情况
 * 小米生成的订单很多，锤子生成的订单却很少，这样导致处理小米订单的reducer压力很大
 * 处理锤子订单的reducer压力很小，这样就导致了数据倾斜
 * 可以通过直接在map端进行join，不采用reducer来解决问题,将产品表初始化到每一个mapper处理的工作目录中
 * 因为map读取的是文件的切片，切片的数据分布可以认为是平均的
 * <p>
 * Created by zhenglian on 2017/11/12.
 */
public class MapJoin {

    static class MJMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        private Map<String, String> products = new HashMap<>();
        private Text k = new Text();
        /**
         * mapper运行时先调用setup方法进行一些初始化，然后调用map方法进行数据文件map处理，最后调用cleanup清理内存
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 这里获取到工作目录中的产品文件，并进行解析
            URI uri = context.getCacheFiles()[0];
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(uri))));
            String line;
            while (StringUtils.isNotEmpty(line=reader.readLine())) {
                String[] arrs = line.split(",");
                products.put(arrs[0], arrs[1]);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] arrs = line.split(",");
            String productName = products.get(arrs[2]);
            k.set(line + "," + productName);
            context.write(k, NullWritable.get());
        }
    }


    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(MapJoin.class);
        
        job.setMapperClass(MJMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        Path outpath = new Path("C:/Users/Administrator/Desktop/hadooptest/output2");

        FileSystem fs = FileSystem.get(conf);
        
        if(fs.exists(outpath)) {
            fs.delete(outpath, true);
        }

        // 这里不需要reducer来处理数据了，所有的处理过程都交由mapper来进行
        job.setNumReduceTasks(0);
        
        FileInputFormat.setInputPaths(job, new Path("C:/Users/Administrator/Desktop/hadooptest/input2/orders"));
        FileOutputFormat.setOutputPath(job, outpath);

//        job.addArchiveToClassPath(path); // 将jar包添加到classpath
//        job.addCacheArchive(uri); // 将jar包添加到工作目录
//        job.addFileToClassPath(path); // 将文件添加到classpath
        job.addCacheFile(new URI("file:/C:/Users/Administrator/Desktop/hadooptest/input2/product/pdfs.txt"));// 将文件添加到工作目录

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
    
    
}
