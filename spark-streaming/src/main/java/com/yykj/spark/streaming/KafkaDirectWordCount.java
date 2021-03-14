package com.yykj.spark.streaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class KafkaDirectWordCount {

	public static void main(String[] args) throws InterruptedException {
		
		//创建Spark对象
		SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("KafkaDirectWordCount");
		JavaStreamingContext jscc = new JavaStreamingContext(conf, Durations.seconds(5));
		
		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", "node21:9092,node21:9092,node21:9092");
		
		HashSet<String> topicSet = new HashSet<String>();
		topicSet.add("first");
		
		JavaPairInputDStream<String, String> ds_kafka_source = KafkaUtils.createDirectStream(jscc,
				String.class,
				String.class,
				StringDecoder.class,
				StringDecoder.class,
				kafkaParams,
				topicSet);
		
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
				// TODO Auto-generated method stub
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
