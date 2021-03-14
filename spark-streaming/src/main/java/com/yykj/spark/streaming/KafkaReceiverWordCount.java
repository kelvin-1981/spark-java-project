package com.yykj.spark.streaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

public class KafkaReceiverWordCount {


	public static void main(String[] args) throws InterruptedException {
	
		//创建Spark对象
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("KafkaReceiverWordCount");
		JavaStreamingContext jscc = new JavaStreamingContext(conf, Durations.seconds(5));
		
		//参数1：Kafka topic
		//参数2：接收Kafka数据线程数量
		HashMap<String,Integer> kafkaParams = new HashMap<String,Integer>();
		kafkaParams.put("first", 1);
		
		//Zookeeper
		String zkList = "node21:2181,node22:2181,node23:2181";
		
		//获取kafka数据
		JavaPairReceiverInputDStream<String, String> ds_kafka_source = KafkaUtils.createStream(jscc, zkList, "kafkaSource", kafkaParams);
		
		JavaDStream<String> ds_cala = ds_kafka_source.flatMap(new FlatMapFunction<Tuple2<String,String>, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Tuple2<String, String> tuple) throws Exception {
				return Arrays.asList(tuple._2.split(" ")).iterator();
			}
		});
		
		JavaPairDStream<String, Integer> ds_cala_2 = ds_cala.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		
		JavaPairDStream<String, Integer> ds_cala_3 = ds_cala_2.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		ds_cala_3.print();
		
		jscc.start();
		jscc.awaitTermination();
		jscc.stop();
		jscc.close();
	}
}
