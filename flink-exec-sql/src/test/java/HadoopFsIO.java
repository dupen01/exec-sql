import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Auther: dupeng
 * @Date: 2023/03/18/11:04
 * @Description:
 */
public class HadoopFsIO {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        String localFile = "file:///Users/pengdu/IdeaProjects/flink-exec/Flink-Exec-SQL/test.sql";
        String hdfsFile = "hdfs://192.168.1.6:8020/lakehouse/arc/hello/base/metadata/v2.metadata.json";
        String hdfsFile2 = "hdfs://ns1/lakehouse/arc/hello/base/metadata/v2.metadata.json";
        // System.out.println();
        System.out.println(file2Text(hdfsFile2));
    }

    private static String file2Text(String fullFilePath) throws IOException, URISyntaxException {
        Configuration configuration = new Configuration();
        FileSystem.get(configuration);
        FileSystem fileSystem = FileSystem.get(configuration);
        String filePath = fullFilePath;
        if (fullFilePath.startsWith("hdfs://")) {
            filePath = fullFilePath.replaceAll("^hdfs://.*?(?=/)", "");
            String uri = fullFilePath.replaceAll(filePath, "");
            fileSystem = FileSystem.get(new URI(uri), configuration);
        }
        FSDataInputStream fis = fileSystem.open(new Path(filePath));
        StringBuilder sb = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
        String str ;
        do {
            str = reader.readLine();
            if (str != null){
                sb.append(str).append("\n");
            }
        }while (str != null);
        reader.close();
        fis.close();
        fileSystem.close();
        return sb.toString();
    }
}
