import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import java.util.List;

/**
 * @Auther: dupeng
 * @Date: 2023/10/25/11:10
 * @Description:
 */
public class FlinkFIleSourceTest {
    private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    private static final String filePath = "/Users/pengdu/IdeaProjects/personal-project/exec-sql/flink-exec-sql/src/main/resources/test2.sql";
    public static void main(String[] args) throws Exception {
        // env.setParallelism(1);
        List<String> textLineList = null;
        FileSource<String> source = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(filePath))
                .build();
        CloseableIterator<String> stringCloseableIterator = env
                .fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "sql-file-source")
                .collectAsync();
        env.execute();
        while (stringCloseableIterator.hasNext()){
            System.out.println("---->");
            System.out.println(stringCloseableIterator.next());
        }

        env.close();
    }
}
