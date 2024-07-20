package top.anets.module.sparkSql;

import org.apache.hive.jdbc.HiveStatement;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.columnar.LONG;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 参考:https://blog.csdn.net/syhiiu/article/details/139794412
 */
public class SparkSqlDemo {
    public static void main(String[] args) throws  Exception {
//        testJdbcDataFrame();
//        query();
//        queryServe();
        testFetchJson();
    }


    public static void testFetchJson() throws AnalysisException {
        //        创建SparkSession
        SparkSession sparkSession = SparkSession.builder()
                .appName("sql")
                .master("local")
//                .config("spark.master", "spark://192.168.64.3:7077")
                .getOrCreate();
//        获取数据
        Dataset<Row> json = sparkSession.read().json("data/json");
        json.createOrReplaceTempView("testtable");
//        执行SQL
        sparkSession.sql("select name from testtable").show();
//        sparkSession.sql("create table test_table as select name from testtable");
//        关闭SparkSession
        sparkSession.stop();
    }


    /**
     * 非JSON格式的Dataset创建
     *
     * 上面创建Dataset时使用了最简单的json，因为json自己带有schema结构，因此不需要手动去增加，如果是一个txt文件，就需要在创建Dataset时手动塞入schema。
     */
    public static void testFetchTxt(){
        SparkSession sparkSession = SparkSession.builder()
                .appName("sql")
                .master("local")
                .getOrCreate();
        SparkContext sparkContext = sparkSession.sparkContext();
        JavaSparkContext sc = new JavaSparkContext(sparkContext);
        JavaRDD<String> lines = sc.textFile("data/user.txt");
        //将String类型转化为Row类型
        JavaRDD<Row> rowJavaRDD = lines.map(new Function<String, Row>() {
            @Override
            public Row call(String v1) throws Exception {
                String[] split = v1.split(" ");
                return RowFactory.create(
                        split[0],
                        Integer.valueOf(split[1])
                );
            }
        });
        //定义schema
        List<StructField> structFields = Arrays.asList(
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true)
        );
        StructType structType = DataTypes.createStructType(structFields);
        //生成dataFrame
        Dataset<Row> dataFrame = sparkSession.createDataFrame(rowJavaRDD, structType);
        dataFrame.show();

    }


    /**
     * 通过JDBC创建DataFrame
     *
     */
    public static void testJdbcDataFrame(){
        SparkSession sparkSession = SparkSession.builder()
                .appName("sql")
                .master("local")
                .getOrCreate();
        Map<String,String> options = new HashMap<>();
        options.put("url","jdbc:mysql://rtatiscs");
        options.put("driver","com.mysql.jdbc.Driver");
        options.put("user","stacs");
        options.put("password","@");
        options.put("dbtable","abadata4");

        long start = System.currentTimeMillis();
        Dataset<Row> jdbc = sparkSession.read().format("jdbc").options(options).load();
        jdbc.createOrReplaceTempView("abadata4");
        System.out.println("=========加载用时:"+(System.currentTimeMillis() - start));
        System.out.println("========================"+jdbc.count());
        long start2 = System.currentTimeMillis();
        sparkSession.sql("select count(*) from abadata4").show();
        System.out.println("=========查询用时:"+(System.currentTimeMillis() - start2));//83s
        sparkSession.close();
        System.out.println("完成===============================");
    }


    public static void query(){
        SparkSession sparkSession = SparkSession.builder()
                .appName("sql")
                .master("local")
                .getOrCreate();
        long start2 = System.currentTimeMillis();
        sparkSession.sql("select count(*) from abadata4").show();
        System.out.println("=========查询用时:"+(System.currentTimeMillis() - start2));//83s
        sparkSession.close();
        System.out.println("完成===============================");
    }


    public static void queryServe() throws SQLException, ClassNotFoundException {
        HiveStatement stmt = null;
        String url = "jdbc:hive2://192.168.64.3:10001/";
        //thrift开启http模式
//        String url = "jdbc:hive2://11.51.196.255:10002/default?hive.server2.transport.mode=http;hive.server2.thrift.http.path=cliservice";
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection conn = DriverManager.getConnection(url, "", "");
        stmt = (HiveStatement) conn.createStatement();

        String sql = "select * from db01.t1 order by id";
        System.out.println("Running" + sql);
        ResultSet res = stmt.executeQuery(sql);


        while (res.next()) {
            System.out.println("id: " + res.getInt(1) + "\tname: " + res.getString(2) + "\tage: " + res.getString(3));
        }
    }

}
