package com.zhusheng.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class TableDemo {
	private static Configuration conf = null;

	public static void main(String[] args) throws Exception {
		// 指定zookeeper集群信息，配置 $HBASE_HOME/conf/hbase-site.xml
		conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "huatec05:2181,huatec06:2181,huatec07:2181");
		
		/**
		 * 表操作
		 */
		//createTable();
		//dropTable();
		
		/**
		 * 增删改查
		 */
		
		//put();
		//putAll();
		get();
		//scan();
		//delete();
	}

	/**
	 * 创建表
	 * 
	 * @throws Exception
	 */
	public static void createTable() throws Exception {
		// 1、创建一个HBase连接对象
		Connection conn = ConnectionFactory.createConnection(conf);

		// 2、获取一个操作数据表的对象
		Admin admin = conn.getAdmin();

		// 创建表和列族
		HTableDescriptor table = new HTableDescriptor(TableName.valueOf("users"));
		HColumnDescriptor family_hcd = new HColumnDescriptor("info");
		HColumnDescriptor family_data = new HColumnDescriptor("data");
		table.addFamily(family_hcd);
		table.addFamily(family_data);

		//3、检查表是否存在
		TableName tableName = table.getTableName();
		if (admin.tableExists(tableName)) {
			System.out.println("表已经存在");
			return;
		}
		// 4、创建表
		admin.createTable(table);
		System.out.println("创建表成功");

		// 5、关闭连接
		admin.close();
		conn.close();
	
	}
	/**
	 * 删除表
	 * @throws Exception
	 */
	public static void dropTable() throws Exception {
		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
		TableName tableName = TableName.valueOf("users");
		
		//Disable an existing table  
		admin.disableTable(tableName);
		//Delete a table (Need to be disabled first)  
		admin.deleteTable(tableName);
		System.out.println("删除表成功");
	}
	
	/**
	 * 插入数据
	 */
	public static void put()throws Exception{
		HTable htable = new HTable(conf, "users");
		
		Put put = new Put(Bytes.toBytes("rk0001"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("Tom Smith"));
		htable.put(put);
		System.out.println("插入成功");

		htable.close();
	}
	/**
	 * 插入100万条，测试时长为：
	 */
	public static void putAll() throws Exception{
		System.out.println("开始插入！");
		HTable htable = new HTable(conf, "users");
		
		//创建集合的时候就指定集合的大小。如果不指定集合的大小，集合默认大小是16，然后每次扩大1.5倍，这样自动扩大到一百万更耗时间。
		List<Put> puts = new ArrayList<>(10000);
		for(int i = 1; i<1000001; i++){
			Put put = new Put(Bytes.toBytes("rk" + i));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("number"), Bytes.toBytes(i + ""));
			puts.add(put);
			
			//每当集合装满10000时，提交一次
			if(i % 10000 ==0){
				htable.put(puts);
				//clear的效率没有直接new的效率高
				puts = new ArrayList<>(10000);
			}
		}
		//再提交一次，避免出现最后集合不满10000条
		htable.put(puts);
		System.out.println("插入完成！");
		htable.close();
	}
	/**
	 * 查询
	 * @throws Exception
	 */
	public static void get() throws Exception{
		HTable htable = new HTable(conf, "users");
		Get get = new Get(Bytes.toBytes("rk49999"));
		//get.setMaxVersions(5);
		Result result = htable.get(get);
		if(!result.isEmpty()){
			List<KeyValue> list = result.list();
			for(KeyValue kv:list){
				System.out.println("family-->"+new String(kv.getFamily()));
				System.out.println("qualifier-->" +new String(kv.getQualifier()));
				System.out.println("value-->" + new String(kv.getValue()));
			}
		}else{
			System.out.println("不存在");
		}
		
		htable.close();
	}
	/**
	 * 区间查询
	 * @throws Exception
	 */
	public static void scan() throws Exception{
		HTable htable = new HTable(conf, "users");
		Scan scan = new Scan(Bytes.toBytes("rk49990"), Bytes.toBytes("rk50000"));
		ResultScanner results = htable.getScanner(scan);
		
		for(Result re:results){
			byte[] value = re.getValue(Bytes.toBytes("info"), Bytes.toBytes("number"));
			System.out.println(Bytes.toString(value));
		}
		htable.close();
	}
	/**
	 * 删除
	 * @throws Exception
	 */
	public static void delete() throws Exception{
		HTable htable = new HTable(conf, "users");
		Delete delete = new Delete(Bytes.toBytes("rk49999"));
		delete.deleteColumn(Bytes.toBytes("info"), Bytes.toBytes("number"));
		htable.delete(delete);
		System.err.println("删除成功");
		
		htable.close();
		
	}
}











