import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * @author delia
 * @create 2016-09-26 上午11:16
 */

public class SparkDemoHDFS {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkDemoLocal");//SparkConf对象包含了Spark应用的一些列信息
        JavaSparkContext sc = new JavaSparkContext(conf);//初始化spark,创建SparkContext对象,指定spark访问集群的方式.
        JavaRDD<String> rddFile = sc.textFile("/user/hdfs/rawlog/www_sinaedgeahsolci14ydn_trafficserver/2016_09_23/00/www_sinaedgeahsolci14ydn_trafficserver-yf235028.scribe.dip.sina.com.cn_16469-2016_09_23_00-20160923011_00000 ");//该数据集没有加载到内存，rddFile仅仅是一个指向文件的指针。

        JavaRDD<Integer> lineLengths =rddFile.map(s->s.length());//此时lineLengths也没有进行运算，因为map操作为懒执行。
        //lineLengths.persist(StorageLevel.MEMORY_ONLY());//将之缓存到内存,供其他计算使用.否则会每次重新计算
        lineLengths.cache();//使用默认存储级别的快捷设置方法.默认StorageLevel.MEMORY_ONLY,将反序列化的对象存储到内存中.
        int totalLength = lineLengths.reduce((a,b)->a+b);

        //<K,V>形式的RDD
        JavaPairRDD<String,Integer> pairs = rddFile.mapToPair(s->new Tuple2(s,1));//转换为<K,V>格式的RDD
        pairs.foreach(a->{System.out.println(a);});
        JavaPairRDD<String,Integer> counts = pairs.reduceByKey((a,b)->a+b);
        //JavaPairRDD<String,Integer> counts = pairs.sortByKey();
        counts.foreach(a->{System.out.println(a);});
        sc.stop();
    }
}
