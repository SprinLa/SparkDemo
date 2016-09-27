import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author delia
 * @create 2016-09-27 上午11:42
 */

public class SparkDemo {
    //public static void wordCount(){
    //    JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local"));
    //    JavaRDD<String> lines = sc.textFile("/Users/delia/Desktop/data.txt");
    //    JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>(){
    //
    //        @Override
    //        public Iterable<String> call(String line) throws Exception {
    //            return Arrays.asList(line.split("\t"));
    //        }
    //    });
    //
    //    JavaRDD<String> worsd = lines.flatMap(s->Arrays.asList((String[])s.split(" ")));
    //}
    public static void main(String[] args) {
        //SparkConf conf = new SparkConf().setAppName("SparkDemo");//SparkConf对象包含了Spark应用的一些列信息
        JavaSparkContext sc = new JavaSparkContext(new SparkConf());//初始化spark,创建SparkContext对象,指定spark访问集群的方式.
        //该数据集没有加载到内存，rddFile仅仅是一个指向文件的指针。
        JavaRDD<String> rddFile = sc.textFile("/user/hdfs/rawlog/www_sinaedgeahsolci14ydn_trafficserver/2016_09_23/00/");

        JavaRDD<Integer> lineLengths =rddFile.map(s->s.length());//此时lineLengths也没有进行运算，因为map操作为懒执行。
        int totalLength = lineLengths.reduce((a,b)->a+b);
        //lineLengths.cache();//使用默认存储级别的快捷设置方法.默认StorageLevel.MEMORY_ONLY,将反序列化的对象存储到内存中.
        System.out.println("====SparkDemo:totalLength="+totalLength);
        //<K,V>形式的RDD
        //JavaPairRDD<String,Integer> pairs = rddFile.mapToPair(s->new Tuple2(s,1));//转换为<K,V>格式的RDD
        //pairs.foreach(a->{System.out.println(a);});
        //JavaPairRDD<String,Integer> counts = pairs.reduceByKey((a,b)->a+b);
        //JavaPairRDD<String,Integer> counts = pairs.sortByKey();
        //counts.foreach(a->{System.out.println(a);});
        sc.stop();
    }
}
