package com.yykj.spark.streaming;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class StreamingOutputMySQL {

	public static void main(String[] args) throws InterruptedException {
		// 创建Spark对象
		SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("StreamingHDFSWordCount");
		@SuppressWarnings("resource")
		JavaStreamingContext jscc = new JavaStreamingContext(conf, Durations.seconds(5));

		// 监控HDFS数据源（目录）
		JavaDStream<String> ds_source = jscc.textFileStream("hdfs://node21:9000/spark/streaming/wordCountDir");

		// DStream算子
		JavaDStream<String> ds_cala = ds_source.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" ")).iterator();
			}
		});

		// DStream算子
		JavaPairDStream<String, Integer> ds_cala_2 = ds_cala.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});

		// DStream算子
		JavaPairDStream<String, Integer> ds_cala_3 = ds_cala_2.reduceByKey(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		//输出至数据库
		ds_cala_3.foreachRDD(new VoidFunction<JavaPairRDD<String,Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<String, Integer> wordRDD) throws Exception {
				// 保存数据至MySQL
				wordRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Integer>>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void call(Iterator<Tuple2<String, Integer>> tuples) throws Exception {
		
						while (tuples.hasNext()) {
							Tuple2<String, Integer> tuple_info = tuples.next();
							String sql_command = "insert into wordcount(word_name,word_count) values('" + tuple_info._1 + "'," + tuple_info._2 + ");";
							System.out.println(sql_command);
							
							//可以使用数据库连接池
							Connection conn = null;
							Statement statement = null;
							
							try {
								conn = DriverManager.getConnection("jdbc:mysql://node21:3306/testdb","root","hh96n55g");
								statement = conn.createStatement();
								statement.execute(sql_command);
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
					}
				});
			}
		});

		// 输出 action算组
		ds_cala_3.print();
		
		jscc.start();
		jscc.awaitTermination();
		jscc.stop();
	}

}
