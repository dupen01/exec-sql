import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.types.Types;

import java.util.HashMap;

/**
 * @Auther: dupeng
 * @Date: 2024/02/23/10:01
 * @Description:
 */
public class Demo {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.set("fs.s3a.endpoint", "http://172.20.3.12:9000");
        conf.set("fs.s3a.access.key", "minio");
        conf.set("fs.s3a.secret.key", "12345678");

        HiveCatalog hiveCatalog = new HiveCatalog();
        hiveCatalog.setConf(conf);

        HashMap<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, "s3a://lakehouse/iceberg/warehouse");
        properties.put(CatalogProperties.URI, "thrift://172.20.3.22:9083");
        // properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        hiveCatalog.initialize("iceberg", properties);
        System.out.println(hiveCatalog.name());

        System.out.println(hiveCatalog.listNamespaces());

        if (!hiveCatalog.namespaceExists(Namespace.of("test_db"))) {
            hiveCatalog.createNamespace(Namespace.of("test_db"));
            System.out.println("test_db库已创建");
        }

        // 定义表结构schema
        Schema schema = new Schema(
                //Types.NestedField.required(1, "level", Types.StringType.get()),
                //Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
                //Types.NestedField.required(3, "message", Types.StringType.get()),
                //Types.NestedField.optional(4, "call_stack", Types.ListType.ofRequired(5, Types.StringType.get()))
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.required(3, "birth", Types.StringType.get())
        );

        TableIdentifier name = TableIdentifier.of("test_db", "test1");
        Table table;
        if(!hiveCatalog.tableExists(name)){
            table = hiveCatalog.createTable(name, schema);
        }else{
            // System.out.println("表已经存在");
            table = hiveCatalog.loadTable(name);
        }


        // 写入数据


        // read table
        assert table != null;
        System.out.println(table.newScan().select("id", "name", "birth"));



    }
}
