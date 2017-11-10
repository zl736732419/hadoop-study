package com.zheng.hadoop.mapreduce.mrjoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * mapreduce实现订单表与产品表关联查询
 * 它们之间通过pid进行关联，所以可以将pid作为key进行数据汇聚
 * Created by zhenglian on 2017/11/10.
 */
public class MRJoin {
    
    static class MRJoinMapper extends Mapper<LongWritable, Text, Text, InfoBean>{
//        private InfoBean infoBean = new InfoBean();
        private Text k = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] arrs = line.split(",");
            
            // 根据文件名称判断当前读取的文件是product还是order
            FileSplit split = (FileSplit) context.getInputSplit();
            String fileName = split.getPath().getName();
            String pid;
            InfoBean infoBean = new InfoBean();
            if (fileName.startsWith("product")) { // 产品文件1
                // id	pname	category_id	price
                pid = arrs[0];
                infoBean.set(0,"",pid, 0, arrs[1], arrs[2], Double.parseDouble(arrs[3]), 1);
            } else { // 订单文件0
                // id	date	pid	amount
                pid = arrs[2];
                infoBean.set(Integer.parseInt(arrs[0]), arrs[1], pid, Integer.parseInt(arrs[3]), "", "", 0.0d, 0);
            }
            k.set(pid);
            context.write(k, infoBean);
        }
    }

    static class MRJoinReducer extends Reducer<Text, InfoBean, InfoBean, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<InfoBean> beans, Context context) throws IOException, InterruptedException {
            List<InfoBean> orderBeans = new ArrayList<>();
//            InfoBean pBean = null;
//            for (InfoBean bean : beans) {
//                if (Objects.equals(bean.getFlag(), 0)) {
//                    orderBeans.add(bean);
//                }else {
//                    pBean = bean;
//                }
//            }


            InfoBean pBean = new InfoBean();
            for (InfoBean bean : beans) {
                if (Objects.equals(bean.getFlag(), 0)) { // 订单
                    InfoBean orderBean = new InfoBean();
                    try {
                        BeanUtils.copyProperties(orderBean, bean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    orderBeans.add(orderBean);
                } else { // 产品
                    try {
                        BeanUtils.copyProperties(pBean, bean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            for (InfoBean bean : orderBeans) {
                bean.setPname(pBean.getPname());
                bean.setCategoryId(pBean.getCategoryId());
                bean.setPrice(pBean.getPrice());
                context.write(bean, NullWritable.get());
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 设置job运行的jar在哪里,这里表示与类路径相关，也就是任何地方都可以
        job.setJarByClass(MRJoin.class);

        // 指定本业务job需要使用到的mapper与reducer类
        job.setMapperClass(MRJoinMapper.class);
        job.setReducerClass(MRJoinReducer.class);

        // 设定mapper输出结果类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoBean.class);

        // 设置最终输出结果类型
        job.setOutputKeyClass(InfoBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 指定job输入的原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 指定最终输出结果文件所在目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 将job中配置的相关参数以及job所用的jar提交到yarn运行,verbose=true表示打印出日志信息
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
    
}
