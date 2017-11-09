package com.zheng.hadoop.mapreduce.mrjoin;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 产品订单消息表
 * Created by zhenglian on 2017/11/10.
 */
public class InfoBean implements Writable {
    private Integer orderId;
    private String date;
    private String pid;
    private Integer amount;
    private String pname;
    private String categoryId;
    private Double price;
    /**
     * 用于区分当前bean是产品bean还是订单bean
     * 0-订单
     * 1-产品
     */
    private Integer flag;
    
    public InfoBean() {
        
    }

    public void set(Integer orderId, String date, String pid, Integer amount, String pname, String categoryId, Double price, Integer flag) {
        this.orderId = orderId;
        this.date = date;
        this.pid = pid;
        this.amount = amount;
        this.pname = pname;
        this.categoryId = categoryId;
        this.price = price;
        this.flag = flag;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(orderId);
        output.writeUTF(date);
        output.writeUTF(pid);
        output.writeInt(amount);
        output.writeUTF(pname);
        output.writeUTF(categoryId);
        output.writeDouble(price);
        output.writeInt(flag);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        orderId = input.readInt();
        date = input.readUTF();
        pid = input.readUTF();
        amount = input.readInt();
        pname = input.readUTF();
        categoryId = input.readUTF();
        price = input.readDouble();
        flag = input.readInt();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append(orderId)
                .append(date)
                .append(pid)
                .append(amount)
                .append(pname)
                .append(categoryId)
                .append(price)
                .build();
    }

    public Integer getOrderId() {
        return orderId;
    }

    public void setOrderId(Integer orderId) {
        this.orderId = orderId;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public Integer getAmount() {
        return amount;
    }

    public void setAmount(Integer amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(String categoryId) {
        this.categoryId = categoryId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public Integer getFlag() {
        return flag;
    }

    public void setFlag(Integer flag) {
        this.flag = flag;
    }
}
