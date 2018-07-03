package com.huatec.hdfs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.testng.annotations.Test;

import java.io.*;
import java.net.URI;

/**
 * A Camel Application
 */
public class MainApp {

    /**
     * A main() so we can easily run these routing rules in our IDE
     */
    public static void main(String... args) throws Exception {

    }

    /**
     * 上传
     * @throws Exception
     */
    @Test
    public void testUPload() throws Exception {
        int BUFFER_SIZE = 4096;
        FileSystem fs = FileSystem.get(new URI("hdfs://huatec01:9000"), new Configuration(), "root");
        // 指定输出：hdfs
        FSDataOutputStream out = fs.create(new Path("/mysql-connect-J.jar"));
        // 指定输入：当前系统
        FileInputStream in = new FileInputStream(new File("/Users/zhusheng/tmp/mysql-connector-java-5.1.42.tar"));
        // 执行拷贝
        IOUtils.copyBytes(in, out, BUFFER_SIZE, true);
    }

    /**
     * 下载
     * @throws Exception
     */
    @Test
    public void testDownload() throws Exception{
        int BUFFER_SIZE=4096;
        //获取hdfs文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://huatec01:9000"), new Configuration());
        InputStream in = fs.open(new Path("/wordcount.txt"));
        OutputStream out = new FileOutputStream(new File("/Users/zhusheng/tmp/wordcount.txt"));
        IOUtils.copyBytes(in, out, BUFFER_SIZE);
    }

    /**
     * 删除文件、目录
     * @throws Exception
     */
    @Test
    public void testDel() throws Exception {
        FileSystem fs = FileSystem.get(new URI("hdfs://huatec01:9000"), new Configuration(), "root");
        //boolean flag = fs.delete(new Path("/mysql-connect-J.jar"), true);
        boolean flag = fs.delete(new Path("/td6"), true);
        System.out.println(flag);
    }

    /**
     * 新建目录
     * @throws Exception
     */
    @Test
    public void testMkdir() throws Exception{
        FileSystem fs = FileSystem.get(new URI("hdfs://huatec01:9000"),new Configuration(),"root");
        boolean flag = fs.mkdirs(new Path("/newDir"));
    }

    /**
     * 新建文件
     * @throws Exception
     */
    @Test
    public void createFile() throws Exception {
        FileSystem fs = FileSystem.get(new URI("hdfs://huatec01:9000"), new Configuration(), "root");
        Path f = new Path("/a/readme.txt");
        FSDataOutputStream output = fs.create(f);

        String fileContent = "Please readme before you excute these cmds.";
        byte[] bytes = fileContent.getBytes();
        output.write(bytes);

        System.out.println("new file \t" + new Configuration().get("fs.default.name") + "/a/readme.txt");

    }

    /**
     * 文件列表
     * @throws Exception
     */
    @Test
    public void listFiles() throws Exception {
        FileSystem fs = FileSystem.get(new URI("hdfs://huatec01:9000"), new Configuration(), "root");
        Path f = new Path("/sample");

        FileStatus[] status = fs.listStatus(f);
        for (int i = 0; i < status.length; i++) {
            System.out.println(status[i].getPath().toString());
        }
    }
}

