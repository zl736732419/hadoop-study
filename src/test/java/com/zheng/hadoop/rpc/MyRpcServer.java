package com.zheng.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

import java.io.IOException;

/**
 * hadoop rpc远程调用框架
 * 完全可以用于自己的项目业务场景中，根本不需要安装hadoop环境
 * Created by zhenglian on 2017/10/30.
 */
public class MyRpcServer {
    public static void main(String[] args) throws IOException {
        Server server = new RPC.Builder(new Configuration())
                .setBindAddress("localhost")
                .setPort(8888)
                .setProtocol(MyRpcServerProtocol.class)
                .setInstance(new MyRpcServerHandler())
                .build();
        server.start();
    }
}
