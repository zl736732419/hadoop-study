package com.zheng.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by zhenglian on 2017/10/30.
 */
public class MyRpcClient {
    public static void main(String[] args) throws IOException {
        MyRpcServerProtocol client = RPC.getProxy(MyRpcServerProtocol.class, 1L, new InetSocketAddress("localhost", 8888), 
                new Configuration());
        String result = client.getMetadata("/anglababy");
        System.out.println(result);
    }
}
