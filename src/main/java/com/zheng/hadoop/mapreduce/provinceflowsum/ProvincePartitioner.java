package com.zheng.hadoop.mapreduce.provinceflowsum;

import com.zheng.hadoop.mapreduce.flowsum.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 自定义分区实现，
 * 按照手机号所属不同归属地进行分区
 * KEY, VALUE 对应map输出的结果类型
 * Created by zhenglian on 2017/11/5.
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean>{
    
    // 这里模拟预先加载手机号归属地逻辑
    private static Map<String, Integer> provinceDict = new HashMap<>();
    
    static {
        provinceDict.put("136", 0); // 0,1,2,3分别代表不同分区号
        provinceDict.put("137", 1);
        provinceDict.put("138", 2);
        provinceDict.put("139", 3);
    }
    
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String mobile = text.toString();
        String prefix = mobile.substring(0, 3);
        Integer partitionNum = provinceDict.get(prefix);
        partitionNum = Optional.ofNullable(partitionNum).orElse(4);
        return partitionNum;
    }
}
