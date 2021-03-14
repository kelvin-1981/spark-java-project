package com.yykj.spark.core.samples;

import java.util.Arrays; 
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class TopN {

	public static void main(String[] args) {
		
		//创建Spark对象
		SparkConf conf = new SparkConf().setMaster("local").setAppName("TopN");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//获取数据
		List<Integer> source = Arrays.asList(100,53,23,65,87,54,98,56,78,99);
		JavaRDD<Integer> rdd_source = sc.parallelize(source);
		
		//转换成为KV
		JavaPairRDD<Integer, Integer> rdd_cala = rdd_source.mapToPair(new PairFunction<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, Integer> call(Integer t) throws Exception {
				return new Tuple2<Integer, Integer>(t, t);
			}
		});
		
		//按Key进行排序
		JavaPairRDD<Integer, Integer> rdd_cala_2 = rdd_cala.sortByKey(false);
		
		//KV中取数值 是否这样效率低？
		JavaRDD<Integer> rdd_cala_3 = rdd_cala_2.map(new Function<Tuple2<Integer,Integer>, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Tuple2<Integer, Integer> tuple) throws Exception {
				return tuple._2;
			}
		});
		
		//获取TOP 3
		List<Integer> list = rdd_cala_3.take(3);
		
		//输出
		for (Integer info : list) {
			System.out.println(info);
		}
	}

}
