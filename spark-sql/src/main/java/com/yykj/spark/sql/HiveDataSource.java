package com.yykj.spark.sql;

import org.apache.spark.sql.SparkSession;

public class HiveDataSource {

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		
		//创建Spark对象
		//SparkSession spark = SparkSession.builder().master("local").appName("HiveDataSource").enableHiveSupport().getOrCreate();
		SparkSession spark = SparkSession.builder().appName("HiveDataSource").enableHiveSupport().getOrCreate();
		
		spark.sql("use itcast");
		spark.sql("select * from student2").show();
	}
}
