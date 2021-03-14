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
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class StreamingWordCount {

	public static void main(String[] args) throws InterruptedException {
		//创建Spark对象
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("StreamingWordCount");
		//Durations.seconds(5) 每隔5秒统计一次，切割生成一个RDD
		JavaStreamingContext jscc = new JavaStreamingContext(conf, Durations.seconds(5));
		
		//DStream包含多个RDD，每隔5秒获取一个RDD
		JavaReceiverInputDStream<String> ds_source = jscc.socketTextStream("node21", 8888);
		
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
		
		//每次打印5秒内单词的统计情况
		ds_cala_3.print();
		
		//启动
		jscc.start();
		jscc.awaitTermination();
		jscc.close();
	}
}
