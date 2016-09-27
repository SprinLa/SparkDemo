/**
 * @author delia
 * @create 2016-09-23 下午2:09
 */

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

public class SparkDemoLocal {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkDemoLocal").setMaster("local");//SparkConf对象包含了Spark应用的一些列信息
        JavaSparkContext sc = new JavaSparkContext(conf);//初始化spark,创建SparkContext对象,指定spark访问集群的方式.
        List<Integer> data = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> distData = sc.parallelize(data);//创建了一个并行集合,数据被复制到一个分布式数据集中,可进行并行操作
        distData.foreach(a-> System.out.println(a));
        int sum = distData.reduce((a,b) -> a+b);
        System.out.println("sum="+sum);

        //共享变量之广播变量
        Broadcast<int[]> broadcast = sc.broadcast(new int[]{1,2,3});
        System.out.print("共享变量之广播变量-broadcast:");
        System.out.println(Arrays.toString(broadcast.value()));

        //共享变量值累加变量(累加器)
        Accumulator<Integer> accumulator = sc.accumulator(0);
        sc.parallelize(Arrays.asList(1,2,3,4)).foreach(x-> accumulator.add(x));
        System.out.print("共享变量之累加变量-accumulator:");
        System.out.println(accumulator.value());
        sc.stop();
    }
}
