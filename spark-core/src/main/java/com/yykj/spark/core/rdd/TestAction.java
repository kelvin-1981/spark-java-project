package com.yykj.spark.core.rdd;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class TestAction {

	public static void main(String[] args) {
		
		//声明Spark对象
		SparkConf conf = new SparkConf().setMaster("local").setAppName("TestTransformaction");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//1.Count算子：用于统计数量条数
		actionCount(sc);
		
		//2.Reduce算子：遍历计算数值
		//actionReduce(sc);
		
		//3.Take算子：拾取数据
		//actionTake(sc);
		
		//4.TakeSample算子：取样并抽取
		//actionTakeSample(sc);
		
		//5.SaveAsTextFile算子：保存数据至文件
		//actionSaveAsTextFile(sc);
		
		//6.CountByKey算子：根据Key计算数据数量
		//shuffle算子
		//actionCountByKey(sc);
	}
	
	/**
	 * CountByKey算子：根据Key计算数据数量
	 * shuffle算子
	 * @param sc
	 */
	public static void actionCountByKey(JavaSparkContext sc){
		List<Tuple2<String, Integer>> values = Arrays.asList(
				new Tuple2<String, Integer>("kelvin",100),
				new Tuple2<String, Integer>("tony",60),
				new Tuple2<String, Integer>("kelvin",100),
				new Tuple2<String, Integer>("sum",30),
				new Tuple2<String, Integer>("tony",40));
		JavaPairRDD<String, Integer> rdd_source = sc.parallelizePairs(values);
		
		Map<String, Long> list = rdd_source.countByKey();
		for (Map.Entry<String, Long> info : list.entrySet()) {
			System.out.println(info.getKey() + " " + info.getValue());
		}
	}
	
	/**
	 * SaveAsTextFile算子：保存数据至文件,目的地目录不能存在
	 * @param sc
	 */
	public static void actionSaveAsTextFile(JavaSparkContext sc){
		
		List<Integer> values = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> rdd_source = sc.parallelize(values,2);
		
		JavaRDD<Integer> rdd_cala_1 = rdd_source.map(new Function<Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1) throws Exception {
				return v1 * 2;
			}			
		});
		
		rdd_cala_1.saveAsTextFile("hdfs://node21:9000/spark/case-01");
		sc.close();
		
		System.out.println("Success!");
	}
	
	/**
	 * TakeSample算子：取样并抽取
	 * @param sc
	 */
	public static void actionTakeSample(JavaSparkContext sc){
		
		List<Integer> values = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> rdd_source = sc.parallelize(values);
		
		List<Integer> list = rdd_source.takeSample(false, 5, 1L);
		System.out.println(list.toString());
	}
	
	/**
	 * Take算子：拾取数据
	 * @param sc
	 */
	public static void actionTake(JavaSparkContext sc){
		
		List<Integer> values = Arrays.asList(1,2,3,4,5,6);
		JavaRDD<Integer> rdd_source = sc.parallelize(values);
		
		List<Integer> list = rdd_source.take(3);
		System.out.println(list.toString());
		
		sc.close();
	}
	
	/**
	 * Reduce算子：遍历计算数值
	 */
	public static void actionReduce(JavaSparkContext sc){
		
		List<Integer> values = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> rdd_source = sc.parallelize(values);
		
		Integer sum = rdd_source.reduce(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		System.out.println(sum);
		
		sc.close();
	}
	
	/**
	 * Count算子：用于统计数量条数
	 */
	public static void actionCount(JavaSparkContext sc){
		
		List<Integer> values = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> rdd_source = sc.parallelize(values);
		
		long count = rdd_source.count();
		System.out.println(count);
	}
}
