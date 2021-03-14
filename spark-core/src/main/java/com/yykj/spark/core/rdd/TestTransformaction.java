package com.yykj.spark.core.rdd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class TestTransformaction {

	/**
	 * Main
	 * @param args
	 */
	public static void main(String[] args) {
		
		//声明Spark对象
		SparkConf conf = new SparkConf().setMaster("local").setAppName("TestTransformaction");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//1.Map算子：Map算子对RDD中数据进行遍历并计算(针对每条数据)
		//transformationMap(sc);
		
		//2.MapPartition算子：一次计算整个Partition中的所有数据
		//使用场景：使用于较少数据量 情况下，不必多次执行算子函数
		//transformationMapPartition(sc);
		
		//3.MapPartitionsWithIndex算子：每次计算一个Partition数据
		//Parition index 作为参数传递
		//transformationMapPartitionsWithIndex(sc);
		
		//4.Filter算子：过滤，通过对RDD计算，当判断结果为true是将结果存入新的RDD
	    //transformationFilter(sc);
		
		//5.Coalesce算子：在一个节点上进行Partirion的减少，压缩
		//应用场景：Filter算子后，减轻数据倾斜
		//transformationCoalesce(sc);
		
		//6.Repartition算子：更改Partition数量，增大缩小都可以
		//Shuffle算子
		//应用场景：当从HDFS读取数据时，Partition根据数据Block进行Partition划分，
		//后续窄依赖操作无法变更Partition个数，造成资源闲置，此时使用此算子增加Partition数量
		//transformationRepartition(sc);
		
		//7.flatMap算子：将RDD中数据进行压平处理
		//transformationFlatMap(sc);
		
		//8.Collect算子：将分布式集群中的数据拉到本地
		//慎用：数据量大时内存溢出
		//transformationCollect(sc);
		
		//9.GroupByKey算子：根据Key值进行全部value合并
		//Shuffle算子
		//transformationGroupByKey(sc);
		
		//10.ReduceByKey算子：GroupByKey + Reduce 此算子于Map端自带Combiner
		//shuffle算子
		//transformationReduceByKey(sc);
		
		//11.AggregateByKey:用于根据Key计算Value数值，可二次运算
		//shuffle算组
		//参数1：Key初始值
		//参数2：shuffle map本地计算函数
		//参数3：全局计算函数
		//transformationAggregateByKey(sc);
		
		//12.Sample算子：数据采样
		//transformationSample(sc);
		
		//13.Distinct算子：去除重复数值
		//shuffle算子
		//transformationDistinct(sc);
		
		//14.SortByKey算子:根据Key排序
		//transformationSortByKey(sc);
		
		//15.Intersection算子：多个RDD交集
		//transformactionIntersection(sc);
		
		//16.Cartesian算子：生成排列组合(笛卡尔乘积)
		//transformactionCartesian(sc);
		
		//17.Cogroup算子：按Key关联两个RDD，将相同Key的Value放入一个集合中
		//transformactionCogroup(sc);
		
		//18.Join算子：根据key进行数据合并
		//shuffle算子
		///transformactionJoin(sc);
		
		//19.FlatMap算子：将RDD中数据进行压平处理
		//transformactionFlatMap(sc);
		
		//20.Union算子：多个RDD并集
		//transformactionUnion(sc);
	}
	
	/**
	 * Union算子：多个RDD并集
	 * @param sc
	 */
	public static void transformactionUnion(JavaSparkContext sc){
		
		List<Integer> values_1 = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> rdd_s1 = sc.parallelize(values_1);
		
		List<Integer> values_2 = Arrays.asList(6,7,8,9,10);
		JavaRDD<Integer> rdd_s2 = sc.parallelize(values_2);
		
		JavaRDD<Integer> rdd_cala = rdd_s1.union(rdd_s2);
		System.out.println(rdd_cala.collect().toString());
	}
	
	/**
	 * FlatMap算子：将RDD中数据进行压平处理
	 * @param sc
	 */
	public static void transformactionFlatMap(JavaSparkContext sc){
		
		List<String> values = Arrays.asList("A B C","D E F","G H I");
		JavaRDD<String> rdd_source = sc.parallelize(values);
		
		JavaRDD<String> rdd_cala_1 = rdd_source.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(t.split(" ")).iterator();
			}
		});
		
		
		rdd_cala_1.foreach(new VoidFunction<String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});
	}
	
	/**
	 * Join算子：根据key进行数据合并
	 * shuffle算子
	 * @param sc
	 */
	public static void transformactionJoin(JavaSparkContext sc){
		
		List<Tuple2<Integer, String>> values_1 = Arrays.asList(
				new Tuple2<Integer, String>(1, "U001"),
				new Tuple2<Integer, String>(2, "U002"),
				new Tuple2<Integer, String>(3, "U003"));
		JavaPairRDD<Integer, String> rdd_s1 = sc.parallelizePairs(values_1);
		 
		List<Tuple2<Integer, String>> values_2 = Arrays.asList(
				new Tuple2<Integer, String>(1, "A"),
				new Tuple2<Integer, String>(2, "B"),
				new Tuple2<Integer, String>(1, "C"),
				new Tuple2<Integer, String>(3, "D"));
		
		JavaPairRDD<Integer, String> rdd_s2 = sc.parallelizePairs(values_2);
		
		JavaPairRDD<Integer, Tuple2<String, String>> rdd_cala_1 = rdd_s1.join(rdd_s2);
		
		rdd_cala_1.foreach(new VoidFunction<Tuple2<Integer,Tuple2<String,String>>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<Integer, Tuple2<String, String>> tuple) throws Exception {
				System.out.println("code:" + tuple._1 + " V1:" + tuple._2._1 + " V2:" + tuple._2._2);
			}
		});
	}
	
	/**
	 * Cogroup算子：按Key关联两个RDD，将相同Key的Value放入一个集合中
	 * shuffle算子
	 * @param sc
	 */
	public static void transformactionCogroup(JavaSparkContext sc){
		List<Tuple2<Integer, String>> values_1 = Arrays.asList(
				new Tuple2<Integer, String>(1, "U001"),
				new Tuple2<Integer, String>(2, "U002"),
				new Tuple2<Integer, String>(3, "U003"));
		JavaPairRDD<Integer, String> rdd_s1 = sc.parallelizePairs(values_1);
		 
		List<Tuple2<Integer, String>> values_2 = Arrays.asList(
				new Tuple2<Integer, String>(1, "A"),
				new Tuple2<Integer, String>(2, "B"),
				new Tuple2<Integer, String>(3, "C"),
				new Tuple2<Integer, String>(1, "D"),
				new Tuple2<Integer, String>(2, "E"),
				new Tuple2<Integer, String>(3, "F"));
		JavaPairRDD<Integer, String> rdd_s2 = sc.parallelizePairs(values_2);
		
		JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<String>>> rdd_cala_1 = rdd_s1.cogroup(rdd_s2);
		
		rdd_cala_1.foreach(new VoidFunction<Tuple2<Integer,Tuple2<Iterable<String>,Iterable<String>>>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<String>>> tuple) throws Exception {
				System.out.println(tuple._1 + " " + tuple._2._1.toString() + " " + tuple._2._2.toString());
			}
		});
	}
	
	
	
	/**
	 * Cartesian算子：生成排列组合(笛卡尔乘积)
	 * @param sc
	 */
	public static void transformactionCartesian(JavaSparkContext sc){
		List<String> values_1 = Arrays.asList("kelvin","tony","sum");
		JavaRDD<String> rdd_s1 = sc.parallelize(values_1);
		List<String> values_2 = Arrays.asList("U001","U002","U003");
		JavaRDD<String> rdd_s2 = sc.parallelize(values_2);
		
		JavaPairRDD<String, String> rdd_cala_1 = rdd_s1.cartesian(rdd_s2);
		
		rdd_cala_1.foreach(new VoidFunction<Tuple2<String,String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, String> t) throws Exception {
				System.out.println(t._1 + " " + t._2);
			}
		});
	}
	
	/**
	 * Intersection算子：多个RDD交集
	 * @param sc
	 */
	public static void transformactionIntersection(JavaSparkContext sc){
		
		List<String> values_1 = Arrays.asList("kelvin","tony","sum");
		JavaRDD<String> rdd_s1 = sc.parallelize(values_1);
		List<String> values_2 = Arrays.asList("kelvin","U001","U002");
		JavaRDD<String> rdd_s2 = sc.parallelize(values_2);
		
		JavaRDD<String> rdd_cala_1 = rdd_s1.intersection(rdd_s2);
		
		rdd_cala_1.foreach(new VoidFunction<String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});
	}
	
	/**
	 * SortByKey算子:根据Key排序
	 * @param sc
	 */
	public static void transformationSortByKey(JavaSparkContext sc){
		List<Tuple2<String, Integer>> values = Arrays.asList(
				new Tuple2<String, Integer>("kelvin",100),
				new Tuple2<String, Integer>("tony",60),
				new Tuple2<String, Integer>("kelvin",100),
				new Tuple2<String, Integer>("sum",30),
				new Tuple2<String, Integer>("tony",40));
		JavaPairRDD<String, Integer> rdd_source = sc.parallelizePairs(values);
		
		JavaPairRDD<String, Integer> rdd_cala_1 = rdd_source.sortByKey(false);
		System.out.println(rdd_cala_1.collect().toString());
	}
	
	/**
	 * Distinct算子：去除重复数值
	 * shuffle算子
	 * @param sc
	 */
	public static void transformationDistinct(JavaSparkContext sc){
		
		List<String> values = Arrays.asList("kelvin","tony","sum","kelvin");
		JavaRDD<String> rdd_source = sc.parallelize(values,2);
		
		JavaRDD<String> rdd_cala_1 = rdd_source.distinct(5);
		System.out.println(rdd_cala_1.collect().toString());
	}
	
	
	/**
	 * Sample算子：数据采样
	 * 参数1：能否被采样多次
     * 参数2：抽取比例
	 * @param sc
	 */
	public static void transformationSample(JavaSparkContext sc){
		
		List<Integer> values = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> rdd_source = sc.parallelize(values);
		
		JavaRDD<Integer> rdd_cala_1 = rdd_source.sample(false, 0.3);
		System.out.println(rdd_cala_1.collect().toString());
	}
	
	/**
	 * AggregateByKey:用于根据Key计算Value数值，可二次运算
	 * shuffle算子
	 * 参数1：Key初始值
	 * 参数2：shuffle map本地计算函数
	 * 参数3：全局计算函数
	 * @param sc
	 */
	public static void transformationAggregateByKey(JavaSparkContext sc){
		List<Tuple2<String, Integer>> values = Arrays.asList(
				new Tuple2<String, Integer>("kelvin",100),
				new Tuple2<String, Integer>("tony",60),
				new Tuple2<String, Integer>("kelvin",100),
				new Tuple2<String, Integer>("sum",30),
				new Tuple2<String, Integer>("tony",40));
		JavaPairRDD<String, Integer> rdd_source = sc.parallelizePairs(values);
		
		
		JavaPairRDD<String, Integer> rdd_cala_1 = rdd_source.aggregateByKey(0, 
				new Function2<Integer, Integer, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}			
		}, new Function2<Integer, Integer, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
		});
		
		rdd_cala_1.foreach(new VoidFunction<Tuple2<String,Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> tuple) throws Exception {
				System.out.println(tuple._1 + " " + tuple._2);
			}
		});
	}
	
	/**
	 * ReduceByKey算子：GroupByKey + Reduce 此算子于Map端自带Combiner
	 * shuffle算子
	 * 不能用于求平均值
	 * @param sc
	 */
	public static void transformationReduceByKey(JavaSparkContext sc){
		
		List<Tuple2<String, Integer>> values = Arrays.asList(
				new Tuple2<String, Integer>("kelvin",100),
				new Tuple2<String, Integer>("tony",60),
				new Tuple2<String, Integer>("kelvin",100),
				new Tuple2<String, Integer>("sum",30),
				new Tuple2<String, Integer>("tony",40));
		JavaPairRDD<String, Integer> rdd_source = sc.parallelizePairs(values);
		
		JavaPairRDD<String, Integer> rdd_cala_1 = rdd_source.reduceByKey(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		rdd_cala_1.foreach(new VoidFunction<Tuple2<String,Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> tuple) throws Exception {
				System.out.println(tuple._1 + ":" + tuple._2);
			}
		});
	}
	
	/**
	 * GroupByKey算子：根据Key值进行全部value合并
	 * Shuffle算子
	 * @param sc
	 */
	public static void transformationGroupByKey(JavaSparkContext sc){
		
		List<Tuple2<String, Integer>> values = Arrays.asList(
				new Tuple2<String, Integer>("kelvin",100),
				new Tuple2<String, Integer>("tony",60),
				new Tuple2<String, Integer>("kelvin",100),
				new Tuple2<String, Integer>("sum",30),
				new Tuple2<String, Integer>("tony",40));
		JavaPairRDD<String, Integer> rdd_source = sc.parallelizePairs(values);
		System.out.println("RDD_SOURCE Paritions:" + rdd_source.partitions().size());
		
		JavaPairRDD<String, Iterable<Integer>> rdd_cala_1 = rdd_source.groupByKey();
		System.out.println("RDD_CALA_1 Paritions:" + rdd_cala_1.partitions().size());
		
		rdd_cala_1.foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Iterable<Integer>> tuple) throws Exception {
				// TODO Auto-generated method stub
				//System.out.println(tuple._1 + " " + tuple._2);
				Iterator<Integer> iterator = tuple._2.iterator();
				int sum = 0;
				while (iterator.hasNext()) {
					sum += iterator.next();
				}
				System.out.println(tuple._1 + " " + sum);
			}
		});
		
		sc.close();
	}
	
	/**
	 * Collect算子：将分布式集群中的数据拉到本地
	 * 慎用：数据量大时内存溢出
	 * @param sc
	 */
	public static void transformationCollect(JavaSparkContext sc){
		
		List<Integer> values = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> rdd_source = sc.parallelize(values);
		
		JavaRDD<Integer> rdd_cala_1 = rdd_source.map(new Function<Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer value) throws Exception {
				return value * 2;
			}
		});
		
		System.out.println(rdd_cala_1.collect().toString());
		sc.close();
	}
	
	/**
	 * flatMap算子：将RDD中数据进行压平处理
	 * @param sc
	 */
	public static void transformationFlatMap(JavaSparkContext sc){
		
		List<String> values = Arrays.asList("a b c", "d e f","g h i","j k l");
		JavaRDD<String> rdd_source = sc.parallelize(values);
		
		JavaRDD<String> rdd_cala_1 = rdd_source.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(t.split(" ")).iterator(); 
			}
		});
		
		rdd_cala_1.foreach(new VoidFunction<String>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String t) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(t);
			}
		});
		
		sc.close();
	}
	
	/**
	 * Repartition算子：更改Partition数量，增大缩小都可以
	 * Shuffle算子
	 * 应用场景：当从HDFS读取数据时，Partition根据数据Block进行Partition划分，
	 * 后续窄依赖操作无法变更Partition个数，造成资源闲置，此时使用此算子增加Partition数量
	 * @param sc
	 */
	public static void transformationRepartition(JavaSparkContext sc){
		//生成数据算子
		List<String> values = Arrays.asList("U001","U002","U003","U004","U005","U006","U007","U008","U009","U010","U011","U012");
		JavaRDD<String> rdd_source = sc.parallelize(values,3);
		
		JavaRDD<String> rdd_cala_1 = rdd_source.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
				ArrayList<String> list = new ArrayList<String>();
				while (iterator.hasNext()) {
					String name = (String) iterator.next();
					list.add("name:" + name + "|OLD Partition:" + index);
				}
				return list.iterator();
			}
		}, true);
		
		JavaRDD<String> rdd_cala_2 = rdd_cala_1.repartition(6);
		
		JavaRDD<String> rdd_cala_3 = rdd_cala_2.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
				ArrayList<String> list = new ArrayList<String>();
				while (iterator.hasNext()) {
					String name = (String) iterator.next();
					list.add(name + "|NEW Partition:" + index);
				}
				return list.iterator();
			}
		}, true);
		
		rdd_cala_3.foreach(new VoidFunction<String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(String t) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(t);
			}			
		});
		
		sc.close();
	}
	
	/**
	 * Coalesce算子：在一个节点上进行Partirion的减少，压缩
	 * 应用场景：Filter算子后，减轻数据倾斜
	 * @param sc
	 */
	public static void transformationCoalesce(JavaSparkContext sc){
		//生成数据算子
		List<String> values = Arrays.asList("U001","U002","U003","U004","U005","U006","U007","U008","U009","U010","U011","U012");
		JavaRDD<String> rdd_source = sc.parallelize(values,6);
		
		//获取原Partition Index
		JavaRDD<String> rdd_cala_1 = rdd_source.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
				ArrayList<String> list = new ArrayList<String>();
				while (iterator.hasNext()) {
					String name = (String) iterator.next();
					list.add(name + " |OLD PARTITION INDEX:" + index);
				}
				return list.iterator();
			}
		}, true);
		
		//压缩Partirion
		JavaRDD<String> rdd_cala_2 = rdd_cala_1.coalesce(3);
		
		//压缩后的Partition
		JavaRDD<String> rdd_cala_3 = rdd_cala_2.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
				ArrayList<String> list = new ArrayList<String>();
				while (iterator.hasNext()) {
					String name = (String) iterator.next();
					list.add(name + " |NEW PARTITION INDEX:" + index);
				}
				return list.iterator();
			}
		},true);

		//输出
		rdd_cala_3.foreach(new VoidFunction<String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
			
		});
		
		sc.close();
	}
	
	/**
	 * Filter算子：过滤，通过对RDD计算，当判断结果为true是将结果存入新的RDD
	 * @param sc
	 */
	public static void transformationFilter(JavaSparkContext sc){
		
		List<Integer> values = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> rdd_source = sc.parallelize(values);
		
		JavaRDD<Integer> rdd_cala = rdd_source.filter(new Function<Integer, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Integer value) throws Exception {
				if(value % 2 == 0){
					return true;
				}
				else {
					return false;
				}
			}
		});
		
		rdd_cala.foreach(new VoidFunction<Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer value) throws Exception {
				System.out.println(value);
			}
		});
		
		//System.out.println(rdd_cala.collect().toString()); 
		
		sc.close();
	}
	
	/**
	 * MapPartitionsWithIndex算子：每次计算一个Partition数据
	 * Parition index 作为参数传递
	 * @param sc
	 */
	public static void transformationMapPartitionsWithIndex(JavaSparkContext sc){
		
		List<String> names = Arrays.asList("kelvin","tony","sum");
		JavaRDD<String> rdd_source = sc.parallelize(names,2);
		
		JavaRDD<String> rdd_cala = rdd_source.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Integer parIndex, Iterator<String> iterator) throws Exception {
				ArrayList<String> list = new ArrayList<String>();
				while(iterator.hasNext()){
					String name = iterator.next();
					String result = "parition index:" + parIndex + " name:" + name;
					list.add(result);
				}
				return list.iterator();
			}
		}, false);
		
		rdd_cala.foreach(new VoidFunction<String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(String value) throws Exception {
				System.out.println(value);
			}			
		});
		
		sc.close();
	}
	
	/**
	 * MapPartition算子：一次计算整个Partition中的所有数据
	 * 使用场景：使用于较少数据量 情况下，不必多次执行算子函数
	 * @param sc
	 */
	public static void transformationMapPartition(JavaSparkContext sc){
		
		List<String> list = Arrays.asList("kelvin","tony","sum");
		JavaRDD<String> rdd_source = sc.parallelize(list);
		
		//算子内部使用，必须使用final声明
		final HashMap<String, Integer> sorceMap = new HashMap<String,Integer>();
		sorceMap.put("kelvin", 100);
		sorceMap.put("tony", 60);
		sorceMap.put("sum", 10);
		
		//计算
		JavaRDD<Integer> rdd_cala = rdd_source.mapPartitions(new FlatMapFunction<Iterator<String>, Integer>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Integer> call(Iterator<String> iterator) throws Exception {
				List<Integer> list = new ArrayList<>();
				while (iterator.hasNext()) {
					list.add(sorceMap.get(iterator.next()));
				}
				return list.iterator();
			}
		});
		
		//输出
		rdd_cala.foreach(new VoidFunction<Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer t) throws Exception {
				System.out.println("sorce:" + t);
			}
		});
		
		sc.close();
	}
	
	/**
	 * Map算子：Map算子对RDD中数据进行遍历并计算(针对每条数据)
	 * @param sc
	 */
	public static void transformationMap(JavaSparkContext sc){
		
		List<Integer> nums = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> rdd_source = sc.parallelize(nums);
		
		JavaRDD<Integer> rdd_cala = rdd_source.map(new Function<Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public Integer call(Integer value) throws Exception {
				// TODO Auto-generated method stub
				return value * 10;
			}			
		});
		
		rdd_cala.foreach(new VoidFunction<Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer t) throws Exception {
				System.out.println(t);
			}
		});
		
		sc.close();
	}
}
