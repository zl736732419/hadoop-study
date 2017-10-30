package com.zheng.hadoop.rpc;

/**
 * 客户端与namenode(服务器端)之间开放的业务接口
 * Created by zhenglian on 2017/10/30.
 */
public interface MyRpcServerProtocol {
    long versionID = 1L;
    String getMetadata(String path);
}
