package com.yykj.spark.streaming;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class StreamingHDFSWordCount {

	@SuppressWarnings("resource")
	public static void main(String[] args) throws InterruptedException {
		
		//创建Spark对象
		SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("StreamingHDFSWordCount");
		JavaStreamingContext jscc = new JavaStreamingContext(conf, Durations.seconds(5));
		
		//监控HDFS数据源（目录）
		JavaDStream<String> ds_source = jscc.textFileStream("hdfs://node21:9000/spark/streaming/wordCountDir");
		
		//DStream算子
		JavaDStream<String> ds_cala = ds_source.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" ")).iterator();
			}
		});
		
		//DStream算子
		JavaPairDStream<String, Integer> ds_cala_2 = ds_cala.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		
		//DStream算子
		JavaPairDStream<String, Integer> ds_cala_3 = ds_cala_2.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		//输出 action算组
		ds_cala_3.print();
		
		jscc.start();
		jscc.awaitTermination();
		jscc.stop();
	}
}
