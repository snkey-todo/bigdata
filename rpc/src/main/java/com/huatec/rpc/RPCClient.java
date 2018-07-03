package com.huatec.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by zhusheng on 2017/12/29.
 */
public class RPCClient {
    public static void main(String[] args) throws IOException {
        Barty barty = RPC.getProxy(Barty.class, 100,
                new InetSocketAddress("localhost", 2367),
                new Configuration());
        String result = barty.sayHi("Amy");
        System.out.println(result);
    }
}