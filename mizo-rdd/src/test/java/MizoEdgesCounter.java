import mizo.core.MizoEdge;
import mizo.rdd.MizoBuilder;
import mizo.rdd.MizoRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

/**
 * Created by imrihecht on 12/10/16.
 */
public class MizoEdgesCounter {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Mizo Edges Counter")
                .setMaster("local[1]")
                .set("spark.executor.memory", "4g")
                .set("spark.executor.cores", "1")
                .set("spark.rpc.askTimeout", "1000000")
                .set("spark.rpc.frameSize", "1000000")
                .set("spark.network.timeout", "1000000")
                .set("spark.rdd.compress", "true")
                .set("spark.core.connection.ack.wait.timeout", "6000")
                .set("spark.driver.maxResultSize", "100m")
                .set("spark.task.maxFailures", "20")
                .set("spark.shuffle.io.maxRetries", "20");

        SparkContext sc = new SparkContext(conf);

        MizoRDD<MizoEdge> edgeRDD = new MizoBuilder()
//            .logConfigPath("log4j.properties")
            .titanConfigPath("/tmp/janus.properties")
            .regionDirectoriesPath("hdfs://10.32.181.180/apps/hbase/data/data/default/janus-test-graph/*/e")
            .parseInEdges(v -> false)
            .edgesRDD(sc);

        Object edges = edgeRDD.take(100);

        long count = new MizoBuilder()
                .logConfigPath("log4j.properties")
                .titanConfigPath("janus.properties")
                .regionDirectoriesPath("hdfs://my-graph/*/e")
                .parseInEdges(v -> false)
                .edgesRDD(sc)
                .toJavaRDD()
                .count();

        System.out.println("Edges count is: " + count);
    }
}
