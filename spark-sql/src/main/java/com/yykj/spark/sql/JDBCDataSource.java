package com.yykj.spark.sql;

import java.util.HashMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class JDBCDataSource {

	public static void main(String[] args) {
		
		//创建Spark对象
		SparkSession spark = SparkSession.builder().master("local").appName("JDBCDataSource").getOrCreate();
				
		//读取ODBC数据源
		HashMap<String, String> opts = new HashMap<>();
		//opts.put("driver", "com.mysql.jdbc.Driver");
		opts.put("url", "jdbc:mysql://node21:3306/testdb");
		opts.put("dbtable", "student");
		opts.put("user", "root");
		opts.put("password", "hh96n55g");
		
		Dataset<Row> df_student = spark.read().format("jdbc").options(opts).load();
		//df_student.show();
		
		opts.put("dbtable", "sorce");
		Dataset<Row> df_sorce = spark.read().format("jdbc").options(opts).load();
		//df_sorce.show();
	
		//Join
		JavaPairRDD<String, Tuple2<Integer, Integer>> rdd_join = df_student.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Row row) throws Exception {
				return new Tuple2<String, Integer>(row.getString(1), row.getInt(2));
			}
		}).join(df_sorce.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Row row) throws Exception {
				return new Tuple2<String, Integer>(row.getString(0), row.getInt(1));
			}
		}));
		
		rdd_join.foreach(new VoidFunction<Tuple2<String,Tuple2<Integer,Integer>>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Tuple2<Integer, Integer>> tuple) throws Exception {
				System.out.println(tuple._1 + " " + tuple._2._1 + " " + tuple._2._2);
			}
		});
	}
}
