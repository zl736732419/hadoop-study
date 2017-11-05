package com.zheng.hadoop.mapreduce.flowsum.flowdesc;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 序列化自定义bean,用于在hadoop集群中传输
 * Created by zhenglian on 2017/11/5.
 */
public class FlowDescBean implements WritableComparable<FlowDescBean> {

    /**
     * 上行流量
     */
    private Long upFlow;
    /**
     * 下行流量
     */
    private Long downFlow;

    /**
     * 总流量
     */
    private Long sumFlow;
    
    public FlowDescBean() {
    }
    
    public FlowDescBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    public void set(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }
    
    /**
     * 将自定义bean序列化到流中进行传递
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    /**
     * 从流中取出数据，反序列化成bean
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }

    @Override
    public String toString() {
        return this.getUpFlow() + "\t" + this.getDownFlow() + "\t" + this.getSumFlow();
    }

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public int compareTo(FlowDescBean o) {
        // 逆序
        return o.getSumFlow().compareTo(this.getSumFlow());
    }
}
