import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Auther: dupeng
 * @Date: 2023/12/03/09:09
 * @Description:(?<=begin).*(?=end)
 */
public class StringRegexpTest {
    public static void main(String[] args) {
        String testSql = "-- select now(), '${a}';\n" +
                "--\n" +
                "-- -- create function myudf as xxx using xxx.py;\n" +
                "--\n" +
                "-- select xtrim('  he  llo     ');\n" +
                "\n" +
                "CREATE CATALOG paimon WITH (\n" +
                "    'type' = 'paimon',\n" +
                "    'warehouse' = 's3://lakehouse/paimon/warehouse',\n" +
                "    'lock.enabled' = 'true',\n" +
                "    's3.endpoint' = 'http://172.20.3.12:9000',\n" +
                "    's3.access-key' = 'minio',\n" +
                "    's3.secret-key' = '12345678'\n" +
                ");\n" +
                "\n" +
                "EXECUTE   STATEMENT   SET\n" +
                "    BEGIN\n" +
                "insert into paimon.test.t2\n" +
                "select\n" +
                "    id,\n" +
                "    name,\n" +
                "    date_format(ts,'yyyy-MM-dd')\n" +
                "from t1;\n" +
                "insert into paimon.test.t1\n" +
                "select * from paimon.test.t2;\n" +
                "END ;\n";
        int flag = Pattern.MULTILINE | Pattern.DOTALL | Pattern.CASE_INSENSITIVE;
        String string1 = Pattern
                .compile("EXECUTE\\s+STATEMENT\\s+SET\\s+BEGIN", flag)
                .matcher(testSql)
                .replaceAll("");
        String string2 = Pattern.compile("END\\s?;", flag)
                .matcher(string1)
                .replaceAll("");
        System.out.println(string2);

    }
}
