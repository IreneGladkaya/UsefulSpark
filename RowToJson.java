import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local[1]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(jsc.getConf()).getOrCreate();
        SQLContext sqlContext = new SQLContext(jsc);
        jsc.setLogLevel("WARN");

        List<Row> data = Arrays.asList(
                RowFactory.create(1,"hello",12),
                RowFactory.create(1,"test",10),
                RowFactory.create(1,"python",5),
                RowFactory.create(2,"from",16),
                RowFactory.create(2,"make",32),
                RowFactory.create(3,"file",0),
                RowFactory.create(3,"to",1),
                RowFactory.create(3,"system",4),
                RowFactory.create(3,"work",5),
                RowFactory.create(3,"create",15),
                RowFactory.create(3,"delete",21),
                RowFactory.create(3,"insert",87)
        );

        StructType schema = new StructType(new StructField[] {
                new StructField("cluster_number",DataTypes.IntegerType,false,Metadata.empty()),
                new StructField("word",DataTypes.StringType,false,Metadata.empty()),
                new StructField("qnty",DataTypes.IntegerType,false,Metadata.empty()),
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

        df.groupBy("cluster_number").agg(functions.collect_list("word").alias("words"))
                .withColumn("name",functions.to_json(functions.struct(functions.col("words"))))
                .show();

        spark.stop();
    }
}
