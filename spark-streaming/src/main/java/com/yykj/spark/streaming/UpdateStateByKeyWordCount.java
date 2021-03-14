package com.yykj.spark.streaming;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class UpdateStateByKeyWordCount {

	public static void main(String[] args) throws InterruptedException {
		
		//创建Spark对象
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateByKeyWordCount");
		JavaStreamingContext jscc = new JavaStreamingContext(conf, Durations.seconds(5));
		//每次记录上批次状态，需要强行CheckPoint记住state 内存可能会丢失
		jscc.checkpoint(".");
		
		//监听端口数据
		JavaReceiverInputDStream<String> ds_source = jscc.socketTextStream("node21", 8888);
		
		//DStream算组
		JavaDStream<String> ds_cala = ds_source.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" ")).iterator();
			}
		});
		
		//DStream算组
		JavaPairDStream<String, Integer> ds_cala_2 = ds_cala.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		
		//参数1：List<Integer>本批次 groupbykey后Key对应的所有value数组
		//参数2：Optional<S>：上批次相同Key的Value数值，可以为null
		//参数3：Optional<S>：经过逻辑计算后返回的数据
		JavaPairDStream<String, Integer> ds_cala_3 = ds_cala_2.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
				
				Integer pre_value = 0;
				if(state.isPresent()){
					//上批次计算有对应Key数值
					pre_value += state.get();
				}
				
				for (Integer value : values) {
					pre_value += value;
				}
				
				return Optional.of(pre_value);
			}
		});
		
		//输出
		ds_cala_3.print();
		
		jscc.start();
		jscc.awaitTermination();
		jscc.stop();
		jscc.close();
	}
}
