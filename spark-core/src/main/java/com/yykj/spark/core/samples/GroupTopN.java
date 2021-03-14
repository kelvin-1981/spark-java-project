package com.yykj.spark.core.samples;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class GroupTopN {

	public static void main(String[] args) {
		
		System.out.println("Start......");
		//创建Spark对象
		SparkConf conf = new SparkConf().setMaster("local").setAppName("GroupTopN");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//数据源
		List<Tuple2<String, Integer>> source = Arrays.asList(
				new Tuple2<String, Integer>("beijing", 100),
				new Tuple2<String, Integer>("shanghai",60),
				new Tuple2<String, Integer>("shanghai", 59),
				new Tuple2<String, Integer>("beijing", 99),
				new Tuple2<String, Integer>("shanghai", 58),
				new Tuple2<String, Integer>("shanghai", 12),
				new Tuple2<String, Integer>("beijing", 98),
				new Tuple2<String, Integer>("shanghai", 13),
				new Tuple2<String, Integer>("beijing", 43));
		JavaPairRDD<String, Integer> rdd_source = sc.parallelizePairs(source);
		
		//分组
		JavaPairRDD<String, Iterable<Integer>> rdd_cala = rdd_source.groupByKey();
		
		//TOP 3
		JavaPairRDD<String, Iterable<Integer>> rdd_cala_2 = rdd_cala.mapToPair(new PairFunction<Tuple2<String,Iterable<Integer>>, String, Iterable<Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> tuple) throws Exception {
				
				Integer[] result = {0,0,0};
				
				Iterable<Integer> scores = tuple._2;
				Iterator<Integer> iterator = scores.iterator();
				
				while (iterator.hasNext()) {
					Integer value = iterator.next();
					if(value <= result[2]){
						continue;
					}
					if(value > result[2] && value <= result[1]){
						result[2] = value;
					}
					else if(value > result[1] && value <= result[0]){
						result[2] = result[1];
						result[1] = value;
					}
					else{
						result[2] = result[1];
						result[1] = result[0];
						result[0] = value;
					}
				}
				
				return new Tuple2<String, Iterable<Integer>>(tuple._1,Arrays.asList(result));
			}
		});

		//输出
		rdd_cala_2.foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Iterable<Integer>> tuple) throws Exception {
				int top = 0;
				
				Iterator<Integer> iterator = tuple._2.iterator();
				while (iterator.hasNext()) {
					top += 1;
					Integer value = (Integer) iterator.next();
					System.out.println(top + ": " + tuple._1 + " " + value);
				}
			}
		});
	}
}
