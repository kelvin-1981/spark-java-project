package com.yykj.spark.core.samples;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class Accumulators {

	public static void main(String[] args) {
		
		//创建Spark对象
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Accumulators");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//累加器
		@SuppressWarnings("deprecation")
		Accumulator<Integer> accumulator = sc.accumulator(0, "acc");
		
		List<Integer> values = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> rdd_source = sc.parallelize(values);
		
		rdd_source.foreach(new VoidFunction<Integer>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer t) throws Exception {
				accumulator.add(1);
			}
		});
		
		System.out.println(accumulator.value());
	}
}
