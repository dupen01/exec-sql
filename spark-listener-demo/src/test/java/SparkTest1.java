import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.util.QueryExecutionListener;

/**
 * @Auther: dupeng
 * @Date: 2024/02/05/11:07
 * @Description:
 */
public class SparkTest1 implements QueryExecutionListener {

    @Override
    public void onSuccess(String funcName, QueryExecution qe, long durationNs) {

    }

    @Override
    public void onFailure(String funcName, QueryExecution qe, Exception exception) {

    }
}
