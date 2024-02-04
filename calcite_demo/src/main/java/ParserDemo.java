import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.dialect.PrestoSqlDialect;
import org.apache.calcite.sql.dialect.SparkSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

/**
 * @Auther: dupeng
 * @Date: 2024/02/01/11:51
 * @Description:
 */
public class ParserDemo {
    public static void main(String[] args) throws SqlParseException {
        // 提供初始SQL
        String initSql = "select nvl(id, 999), name from t where id=1 order by id desc limit 100";
        // 生成sql解析配置
        SqlParser.Config spark = SqlParser.config()
                .withParserFactory(SqlParserImpl.FACTORY)
                .withQuotedCasing(Casing.UNCHANGED)
                .withUnquotedCasing(Casing.UNCHANGED)
                .withCaseSensitive(false)
                .withConformance(SqlConformanceEnum.DEFAULT);

        // 创建一个sql解析器
        SqlParser sqlParser = SqlParser.create(initSql, spark);


        // 执行sql解析
        SqlNode sqlNode = sqlParser.parseQuery();
        // SqlNode sqlNode = sqlParser.parseStmt();
        // SqlNode sqlNode1 = sqlParser.parseExpression();

        // 将sqlNode输出为目标sql
        SqlKind kind = sqlNode.getKind();
        System.out.println("sql_kind: "+kind);
        SqlWriterConfig sqlWriterConfig = SqlPrettyWriter.config()
                .withAlwaysUseParentheses(true)
                .withUpdateSetListNewline(false)
                .withQuoteAllIdentifiers(false)
                .withFromFolding(SqlWriterConfig.LineFolding.TALL)
                .withIndentation(0)
                .withKeywordsLowerCase(false)
                .withDialect(SparkSqlDialect.DEFAULT);
                // .withDialect(PrestoSqlDialect.DEFAULT);
                //         .withDialect(OracleSqlDialect.DEFAULT);
        System.out.println(sqlNode.toSqlString(c->sqlWriterConfig).getSql());
    }
}
