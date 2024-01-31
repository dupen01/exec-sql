package com.dupeng.flink.sql;

import org.apache.commons.cli.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Auther: dupeng
 * @Date: 2023/11/30/11:00
 * @Description:
 */
public class ExecFlinkSql {
    private static final Logger logger = LoggerFactory.getLogger(ExecFlinkSql.class);
    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("e", "sql", true, "SQL text");
        options.addOption("i", "init", true, "Init text");
        CommandLine cli = new DefaultParser().parse(options, args);
        String sqlText = cli.getOptionValue("sql");
        String initSql = cli.getOptionValue("init");
        if (!cli.hasOption("sql") || sqlText == null){
            throw new IllegalArgumentException("No SQL statement specified.");
        }
        String[] sqlStatements = parseSqlText(sqlText);
        String[] initSqlStatements = parseSqlText(initSql);
        execFlinkSql(sqlStatements, initSqlStatements);
    }


    private static String[] parseSqlText(String sqlText){
        if (sqlText == null){
            return null;
        }else {
            int flag = Pattern.MULTILINE | Pattern.DOTALL | Pattern.CASE_INSENSITIVE;
                String dropExecuteStr = Pattern
                        .compile("EXECUTE\\s+STATEMENT\\s+SET\\s+BEGIN", flag)
                        .matcher(sqlText)
                        .replaceAll("");
                String dropEndStr = Pattern.compile("END\\s?;", flag)
                        .matcher(dropExecuteStr)
                        .replaceAll("");
                ArrayList<String> sqlStatement = new ArrayList<>(Arrays.asList(dropEndStr.split("\n")));
                return String.join("\n", sqlStatement).split(";\\s*$|;(?=\\s*\\n)|;(?=\\s*--)");
        }
    }

    private static void addJar(StreamTableEnvironment tEnv, String[] sqlStatements){
        ArrayList<String> jarsPathList = new ArrayList<>();
        for (String sql : sqlStatements) {
            sql = sql.replaceAll("--.*", "").trim();
            if (sql.toUpperCase().startsWith("ADD JAR")) {
                sql = sql.replace("'", "");
                Matcher jarPathMatcher = Pattern.compile("(?<=').*(?=')").matcher(sql);
                String jarPath = null;
                if (jarPathMatcher.find()){
                    jarPath = jarPathMatcher.group().trim();
                }
                jarsPathList.add(jarPath);
            }
        }
        if (!jarsPathList.isEmpty()){
            tEnv.getConfig().set("pipeline.jars", String.join(";", jarsPathList));
            logger.info("pipeline.jars:{}", String.join(";", jarsPathList));
        }
    }

    private static void execFlinkSql(String[] sqlStatements, String[] initSqlStatements){
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
        // 加载配置文件中的jar包依赖
        // addJar(tEnv, sqlStatements);
        if(initSqlStatements != null){
            for (String sql : initSqlStatements) {
                sql = sql.replaceAll("--.*", "").trim();
                if (sql.isBlank()){
                    continue;
                }
                try {
                    logger.info("========================= [INIT-SQL] =========================");
                    logger.info(sql);
                    logger.info("========================= [INIT-SQL] =========================");
                    tEnv.executeSql(sql);
                    logger.info("OK");
                }
                catch (RuntimeException e){
                    throw new RuntimeException(String.format("Failed to initialize the SQL: %s", sql), e);
                }
            }
        }
        StatementSet statementSet = tEnv.createStatementSet();
        boolean statementSetFlag = false;
        int sqlId = 0;
        for (String sql : sqlStatements){
            sql = sql.replaceAll("--.*", "").trim();
            if (sql.isBlank()){
                continue;
            }
            if (sql.toUpperCase().startsWith("SET")) {
                sql = sql.replace("'", "");
                Matcher key = Pattern.compile("(?<=[sS][eE][tT] ).*(?=\\s*\\=)").matcher(sql);
                Matcher value = Pattern.compile("(?<=\\=).*").matcher(sql);
                if (key.find() && value.find()){
                    String keyStr = key.group().trim();
                    String valueStr = value.group().trim();
                    if ("TABLE.SQL-DIALECT".equalsIgnoreCase(keyStr)){
                        if ("HIVE".equalsIgnoreCase(valueStr)){
                            tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
                            logger.warn("FLINK SQL 已切换为 \"HIVE\" 方言");
                        } else if ("DEFAULT".equalsIgnoreCase(valueStr)) {
                            tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
                            logger.warn("FLINK SQL 已切换为 \"DEFAULT\" 方言");
                        }
                    }
                    tEnv.getConfig().set(keyStr, valueStr);
                }
            }
            else if (sql.toUpperCase().contains("INSERT")) {
                logger.info("INSERT STATEMENT:\n{}", sql);
                statementSet.addInsertSql(sql);
                statementSetFlag = true;
            }
            else{
                sqlId += 1;
                logger.info("=============================== [SQL-{}] ===============================", sqlId);
                logger.info(sql);
                logger.info("=============================== [SQL-{}] ===============================", sqlId);
                tEnv.executeSql(sql).print();
            }
        }
        if (statementSetFlag){
            statementSet.execute();
        }

    }
}
