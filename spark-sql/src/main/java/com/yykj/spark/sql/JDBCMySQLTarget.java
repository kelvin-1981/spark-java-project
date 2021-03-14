package com.yykj.spark.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;


import scala.Tuple2;

public class JDBCMySQLTarget {

	public static void main(String[] args) {
		
		//创建Spark对象
		//SparkSession spark = SparkSession.builder().master("local").appName("JDBCMySQLTarget").getOrCreate();
		SparkSession spark = SparkSession.builder().appName("JDBCMySQLTarget").getOrCreate();
	
		//初始化数据
		List<String> values = Arrays.asList("U001 1","U002 2","U003 4");
		JavaRDD<String> rdd_source = JavaSparkContext.fromSparkContext(spark.sparkContext()).parallelize(values);
		
		JavaRDD<Tuple2<String, Integer>> rdd_cala = rdd_source.map(new Function<String, Tuple2<String, Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String info) throws Exception {
				String[] fields = info.split(" ");
				return new Tuple2<String, Integer>(fields[0].toString(), Integer.parseInt(fields[1]));
			}
		});
		
		rdd_cala.foreach(new VoidFunction<Tuple2<String,Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> tuple) throws ClassNotFoundException, SQLException{
				Class.forName("com.mysql.jdbc.Driver");
				
				String sql = "insert into student(name,age) values('" + tuple._1 + "','" + tuple._2 + "');";
			
				Connection conn = null;
				Statement statement = null;
				try {
					conn = DriverManager.getConnection("jdbc:mysql://node21:3306/testdb","root","hh96n55g");
					statement = conn.createStatement();
					statement.execute(sql);
				} catch (SQLException e) {
					e.printStackTrace();
				} finally{
					if(statement != null){
						statement.close();
					}
					if(conn != null){
						conn.close();
					}
				}
			}
		});
	}
}
