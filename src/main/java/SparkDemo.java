import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author delia
 * @create 2016-09-27 上午11:42
 */

public class SparkDemo {
    private static final Pattern SPACE = Pattern.compile(" ");
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkDemo").setMaster("local");//SparkConf对象包含了Spark应用的一些列信息
        JavaSparkContext sc = new JavaSparkContext(conf);//初始化spark,创建SparkContext对象,指定spark访问集群的方式.
        //JavaRDD<String> rddFile = sc.textFile("/user/hdfs/rawlog/www_sinaedgeahsolci14ydn_trafficserver/2016_09_23/00/");
        JavaRDD<String> lines = sc.textFile("/Users/delia/Desktop/data.txt");

        JavaRDD<String> words = lines.flatMap(line->Arrays.asList(line.split(" ")).iterator());
        words.foreach(word-> System.out.println(word));
        JavaPairRDD<String,Integer> wordPair = words.mapToPair(w->new Tuple2<String, Integer>(w,1));
        JavaPairRDD<String,Integer> counts = wordPair.reduceByKey((x,y)->x+y);
        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<String, Integer> tuple:output){
            System.out.println(tuple._1()+":"+tuple._2());
            System.out.println(tuple.toString());
        }
        //counts.saveAsTextFile("/Users/delia/Desktop/count.txt");
        counts.saveAsObjectFile("/Users/delia/Desktop/count");
        sc.textFile("/Users/delia/Desktop/count").foreach(s-> System.out.println(s));






        sc.stop();
    }
}
