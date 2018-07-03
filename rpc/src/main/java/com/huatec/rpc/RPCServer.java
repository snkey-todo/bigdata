package com.huatec.rpc;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

import java.io.IOException;
/**
 * Created by zhusheng on 2017/12/29.
 */
public class RPCServer implements Barty{

    public static void main(String[] args) throws HadoopIllegalArgumentException, IOException {
        Server server = new RPC.Builder(new Configuration())
                .setInstance(new RPCServer())
                //本地服务
                .setBindAddress("localhost")
                //远程服务
                //.setBindAddress("huatec01")
                .setPort(2367)
                .setProtocol(Barty.class)
                .build();
        server.start();
    }

    @Override
    public String sayHi(String name) {
        return "HI~" + name;
    }
}
