package com.zheng.hadoop.mapreduce.flowsum.flowdesc.usecombiner;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 序列化自定义bean,用于在hadoop集群中传输
 * Created by zhenglian on 2017/11/5.
 */
public class FlowBeanKey implements WritableComparable<FlowBeanKey> {

    private String mobile;
    /**
     * 总流量
     */
    private Long sumFlow;
    
    public FlowBeanKey() {
    }
    
    public FlowBeanKey(String mobile, Long sumFlow) {
        this.mobile = mobile;
        this.sumFlow = sumFlow;
    }
    
    /**
     * 将自定义bean序列化到流中进行传递
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(mobile);
        out.writeLong(sumFlow);
    }

    /**
     * 从流中取出数据，反序列化成bean
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        mobile = in.readUTF();
        sumFlow = in.readLong();
    }

    public String getMobile() {
        return mobile;
    }

    public void setMobile(String mobile) {
        this.mobile = mobile;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public boolean equals(Object obj) {
        if (null == obj) {
            return false;
        }
        if (!(obj instanceof FlowBeanKey)) {
            return false;
        }
        FlowBeanKey other = (FlowBeanKey) obj;
        return new EqualsBuilder().append(this.mobile, other.getMobile()).build();
    }

    @Override
    public String toString() {
        return mobile;
    }

    @Override
    public int compareTo(FlowBeanKey o) {
        return o.getSumFlow().compareTo(this.getSumFlow());
    }
}
