package com.yykj.spark.core.samples;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

public class SharedVariables {

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		
		//创建Spark对象 
		SparkConf conf = new SparkConf().setMaster("local").setAppName("SharedVariables");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> values = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> rdd_source = sc.parallelize(values);
		
		//广播变量 只读
		final Integer num = 2;
		Broadcast<Integer> broadcast = sc.broadcast(num);
		
		JavaRDD<Integer> rdd_cala = rdd_source.map(new Function<Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1) throws Exception {
				return v1 * broadcast.value();
			}
		});
		
		rdd_cala.foreach(new VoidFunction<Integer>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer t) throws Exception {
				System.out.println(t);
			}
		});
		
	}

}
