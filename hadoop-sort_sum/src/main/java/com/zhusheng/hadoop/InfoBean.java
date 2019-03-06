package com.zhusheng.hadoop;



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.io.WritableComparable;

public class InfoBean implements WritableComparable<InfoBean>{

	private String account;	//账户名称
	private double income;	//收入
	private double expenses;//支出
	private double surplus; //结余BigDecimal
	
	//赋值函数，替代构造函数
	public void set(String account,double income,double expenses){
		this.account = account;
		this.income = income;
		this.expenses = expenses;
		this.surplus = income - expenses;
	}
	//序列化
	public void write(DataOutput out) throws IOException {
		out.writeUTF(account);
		out.writeDouble(income);
		out.writeDouble(expenses);
		out.writeDouble(surplus);
	}

	//反序列化
	public void readFields(DataInput in) throws IOException {
		this.account = in.readUTF();
		this.income = in.readDouble();
		this.expenses = in.readDouble();
		this.surplus = in.readDouble();
	}

	//比较
	public int compareTo(InfoBean o) {
		//收入相同
		if(this.income == o.getIncome()){
			//1表示正序，-1表示倒序
			return this.expenses > o.getExpenses() ? 1 : -1;
		}
		//收入不同，
		return this.income > o.getIncome() ? 1 : -1;
	}
	
	@Override
	public String toString() {
		return  income + "\t" +	expenses + "\t" + surplus;
	}
	
	public String getAccount() {
		return account;
	}

	public void setAccount(String account) {
		this.account = account;
	}

	public double getIncome() {
		return income;
	}

	public void setIncome(double income) {
		this.income = income;
	}

	public double getExpenses() {
		return expenses;
	}

	public void setExpenses(double expenses) {
		this.expenses = expenses;
	}

	public double getSurplus() {
		return surplus;
	}

	public void setSurplus(double surplus) {
		this.surplus = surplus;
	}

}
