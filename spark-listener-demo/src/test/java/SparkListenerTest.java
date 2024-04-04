import org.apache.kyuubi.plugin.lineage.Lineage;
import org.apache.kyuubi.plugin.lineage.dispatcher.OperationLineageSparkEvent;
// import org.apache.kyuubi.plugin.lineage.events.Lineage;
// import org.apache.kyuubi.plugin.lineage.events.OperationLineageEvent;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SparkSession;

import java.util.concurrent.atomic.AtomicReference;

/**
 * @Auther: dupeng
 * @Date: 2024/02/05/11:11
 * spark.sql.queryExecutionListeners=org.apache.kyuubi.plugin.lineage.SparkOperationLineageQueryExecutionListener
 * @Description:
 */
public class SparkListenerTest {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .config("spark.sql.queryExecutionListeners", "org.apache.kyuubi.plugin.lineage.SparkOperationLineageQueryExecutionListener")
                .config("spark.kyuubi.plugin.lineage.dispatchers", "SPARK_EVENT")
                // .config("spark.jars", "/Users/pengdu/Downloads/jars/paimon/paimon-spark-3.4-0.6.0-incubating.jar")
                .config("spark.sql.catalog.paimon","org.apache.paimon.spark.SparkCatalog")
                .config("spark.sql.catalog.paimon.warehouse","file:/Users/pengdu/IdeaProjects/personal-project/exec-sql/spark-listener-demo")
                .appName("lineage_test")
                .getOrCreate();


        // AtomicReference<Lineage> sparkEventLineage = null;
        Lineage lineageEnd = null;

        spark.sparkContext().addSparkListener(new SparkListener() {
            @Override
            public void onOtherEvent(SparkListenerEvent event) {
                if (event instanceof OperationLineageSparkEvent){
                    // System.out.println(((OperationLineageSparkEvent) event).lineage().get().inputTables());
                    ((OperationLineageSparkEvent) event).lineage().foreach(
                            lineage -> {
                                System.out.println(lineage.toString());
                                if(lineage.inputTables().nonEmpty()){
                                    System.out.println(lineage);
                                    // sparkEventLineage.set(lineage);
                                    // sparkEventLineage.set(lineage);
                                    // lineage.columnLineage().
                                    // lineageEnd = lineage;
                                    // return sparkEventLineage;

                                }
                                else {
                                    System.out.println("为空？？？");
                                }
                                return lineageEnd;
                            }
                    );

                }

            }
        });


        String ddl = "create table if not exists paimon.test.t1(id int, name string)";
        String ddl2 = "create table if not exists paimon.test.t2(pid int, age int)";
        String testSql = "select t1.id, t1.name, count(t2.age) as age_sum from paimon.test.t1 t1 join paimon.test.t2 t2 on t1.id = t2.pid group by t1.id, t1.name";

        // spark.sql("create database paimon.test");
        // spark.sql(ddl);
        // spark.sql(ddl2);
        spark.sql(testSql);
        System.out.println(lineageEnd);
    }
}
