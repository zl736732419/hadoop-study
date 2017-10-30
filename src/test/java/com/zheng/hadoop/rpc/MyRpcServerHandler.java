package com.zheng.hadoop.rpc;

/**
 * 模拟的服务器实现
 * 这里充当服务端，提供业务服务
 * Created by zhenglian on 2017/10/30.
 */
public class MyRpcServerHandler implements MyRpcServerProtocol {
    @Override
    public String getMetadata(String path) {
        return path + ": " + "hello world";
    }
}
