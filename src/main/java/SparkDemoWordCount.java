import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author delia
 * @create 2016-09-27 下午4:09
 */

public class SparkDemoWordCount {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf());
        JavaRDD<String> lines = sc.textFile("/user/hdfs/rawlog/www_sinaedgeahsolci14ydn_trafficserver/2016_09_23/00/");
        JavaRDD<String> words = lines.flatMap(s-> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String,Integer> wordPairs = words.mapToPair(w->new Tuple2<String, Integer>(w,1));
        JavaPairRDD<String,Integer> counts = wordPairs.reduceByKey((a,b)->a+b);
        counts.saveAsTextFile("/user/hdfs/rawlog/result");

        sc.stop();
    }
}
