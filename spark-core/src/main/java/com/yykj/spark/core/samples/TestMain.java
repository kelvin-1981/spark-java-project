package com.yykj.spark.core.samples;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TestMain {

	public static void main(String[] args) throws IOException {
		//CreateBigTxt();
		RDDCache();
	}
	
	/**
	 * RDD缓存
	 */
	public static void RDDCache(){
		//创建Spark对象
		SparkConf conf = new SparkConf().setAppName("TestMain").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//读取文件
		//当RDD需要复用时需要持久化
		JavaRDD<String> rdd_source = sc.textFile("e:/big.txt");
		rdd_source.cache();
		//rdd_source.persist(StorageLevel.MEMORY_AND_DISK());
		
		//执行计算1
		//count:1000001
		//cost:1218
		
		//count:1000001
		//cost:2595
		long st_1 = System.currentTimeMillis();
		long count = rdd_source.count();
		System.out.println("count:" + count);
		long et_1 = System.currentTimeMillis();
		System.out.println("cost:" + (et_1 - st_1));
		
		//执行计算1
		//count:1000001
		//cost:924
		
		//count:1000001
		//cost:69
		long st_2 = System.currentTimeMillis();
		long count_2 = rdd_source.count();
		System.out.println("count:" + count_2);
		long et_2 = System.currentTimeMillis();
		System.out.println("cost:" + (et_2 - st_2));
	}
	
	/**
	 * 创建大数据量TXT文件
	 * @throws IOException 
	 */
	public static void CreateBigTxt() throws IOException{
		FileWriter fw = new FileWriter(new File("e:/big.txt"));
		for(int i = 0; i <= 1000000; i++){
			fw.write("Request URL:https://sp2.baidu.com/8LUYsjW91Qh3otqbppnN2DJv/link?url=AZ6wKaIU82OKcMKNLb_qjhu2sN-51QXH" 
					+ "_l0MG1ajRw0bkiIvWADAyuvZ71gjWcaHXJpowmFPQ97jaWH8vn2blaXgVboMmhBqGENtmVqKtG_&query=java%20%E5%86%99txt%E6%96" 
					+ "%87%E4%BB%B6&cb=jQuery110205843061016893083_1592831654131&data_name=recommend_common_merger_online&ie=utf-8&" 
					+ "oe=utf-8&format=json&t=1592831804000&_=1592831654133\r\n");
		}
		fw.flush();
		fw.close();
	}
}
