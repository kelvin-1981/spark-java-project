package com.yykj.spark.streaming;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class TransformOperation {

	public static void main(String[] args) throws InterruptedException {
		
		//创建Spark对象
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("TransformOperation");
		JavaStreamingContext jscc = new JavaStreamingContext(conf, Durations.seconds(10));
		
		//构建黑名单RDD
		ArrayList<Tuple2<String, Boolean>> blacklist = new ArrayList<Tuple2<String, Boolean>>();
		blacklist.add(new Tuple2<String, Boolean>("kelvin", true));
		blacklist.add(new Tuple2<String, Boolean>("tony", true));
		blacklist.add(new Tuple2<String, Boolean>("sam", false));
		
		//构建黑名单RDD
		final JavaPairRDD<String, Boolean> rdd_blacklist = jscc.sparkContext().parallelizePairs(blacklist);
		
		//监控端口获取DS
		JavaReceiverInputDStream<String> ds_source = jscc.socketTextStream("node21", 8888);
		
		//DS算子
		JavaPairDStream<String, String> ds_cala = ds_source.mapToPair(new PairFunction<String, String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(String line) throws Exception {
				return new Tuple2<String, String>(line.split(" ")[2], line);
			}
		});
		
		//参数1：ds_cala每个RDD
		//参数2：返回RDD
		JavaDStream<String> ds_cala_2 = ds_cala.transform(new Function<JavaPairRDD<String,String>, JavaRDD<String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public JavaRDD<String> call(JavaPairRDD<String, String> rdd) throws Exception {
				//黑名单匹配
				JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> rdd_join = rdd.leftOuterJoin(rdd_blacklist);
				
				//过滤黑名单
				JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> rdd_filter = rdd_join.filter(new Function<Tuple2<String,Tuple2<String,Optional<Boolean>>>, Boolean>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
						if(tuple._2._2.isPresent() && tuple._2._2.get()){
							return true;
						}
						return false;
					}
				});
				
				JavaRDD<String> rdd_res = rdd_filter.map(new Function<Tuple2<String,Tuple2<String,Optional<Boolean>>>, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
						return tuple._2._1;
					}
				});
				
				return rdd_res;
			}
		});
		
		ds_cala_2.print();
		
		jscc.start();
		jscc.awaitTermination();
		jscc.stop();
		jscc.close();
	}

}
