package com.huatec.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.junit.Before;
import org.junit.Test;

public class ColumnImpl {
	private static Configuration conf = null;
	private static String TABLE_NAME = "users";

	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "huatec03:2181,huatec04:2181,huatec05:2181");
	}

	public static void main(String[] args) throws Exception {
		addColumnFamily();
		//modifyColumnFamily();
		//delColumnFamily();
	}
	/**
	 * 增加列族
	 * @throws Exception
	 */
	public static void addColumnFamily() throws Exception {
		Connection conn = ConnectionFactory.createConnection(conf);
		// 检查表是否存在
		TableName tablename = TableName.valueOf(TABLE_NAME);
		Admin admin = conn.getAdmin();
		if (!admin.tableExists(tablename)) {
			System.out.println("表不存在");
			System.exit(-1);
		}
		// 增加一个列族
		HColumnDescriptor newColumn = new HColumnDescriptor("mark");
		newColumn.setCompactionCompressionType(Algorithm.GZ);
		newColumn.setMaxVersions(3);
		admin.addColumn(tablename, newColumn);
		System.out.println("增加列族");
		
		admin.close();
		conn.close();

	}
	/**
	 * 更新列族
	 * @throws Exception
	 */
	public static void modifyColumnFamily() throws Exception {
		Connection conn = ConnectionFactory.createConnection(conf);
		// 检查表是否存在
		TableName tablename = TableName.valueOf(TABLE_NAME);
		Admin admin = conn.getAdmin();
		if (!admin.tableExists(tablename)) {
			System.out.println("表不存在");
			System.exit(-1);
		}
		// 更新已有列族
		HTableDescriptor table = admin.getTableDescriptor(tablename);
		HColumnDescriptor existColumn = new HColumnDescriptor("info");
		existColumn.setCompactionCompressionType(Algorithm.GZ);
		existColumn.setMaxVersions(HConstants.ALL_VERSIONS);
		table.modifyFamily(existColumn);
		admin.modifyTable(tablename, table);
		System.out.println("更新列族");
		
		admin.close();
		conn.close();
	}
	/**
	 * 删除列族
	 * @throws Exception
	 */
	public static void delColumnFamily() throws Exception {
		Connection conn = ConnectionFactory.createConnection(conf);
		// 检查表是否存在
		TableName tablename = TableName.valueOf(TABLE_NAME);
		Admin admin = conn.getAdmin();
		if (!admin.tableExists(tablename)) {
			System.out.println("表不存在");
			System.exit(-1);
		}
		// 删除列族
		admin.deleteColumn(tablename, "mark".getBytes());
		System.out.println("删除列族");
		
		admin.close();
		conn.close();
	}

}
