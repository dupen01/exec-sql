package com.dupeng.flink.sql;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Auther: dupeng
 * @Date: 2023/10/25/11:12
 * @Description:
 */
public class ExecFlinkSqlFileV5 {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExecFlinkSqlFileV5.class);
    private static final String versionString = "v2023-10-25";
    private static final String sqlSeparatorRegex = ";\\s*$|;(?=\\s*\\n)|;(?=\\s*--)";
    private static final String keySetRegex = "(?<=[sS][eE][tT] ).*(?=\\s*\\=)";
    private static final String valueSetRegex = "(?<=\\=).*";
    private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    private static final StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

    public static void main(String[] args) throws Exception {
        LOGGER.info(getStartString());
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        if (!params.has("f") || params.get("f") == null) {
            throw new IllegalArgumentException("No SQL file specified.");
        }
        String filePath = params.get("f");
        Collection<String> kv = params.getMultiParameter("d");
        execSqlStmt(parseSqlFile(filePath, kv));
    }

    /**
     * 使用Flink 的文件IO读取远程文件的内容，如果是本地文件则使用本地文件IO
     */
    private static String[] parseSqlFile(String filePath, Collection<String> kv) throws Exception {
        String schema = filePath.split("://")[0];
        List<String> textLineList = new ArrayList<>();
        if (new File(filePath).exists()){
            LOGGER.info("将从本地文件系统读取文件...");
            textLineList = Files.readAllLines(Paths.get(filePath));
        }
        else {
            LOGGER.info("将从远程文件 '{}' 系统读取文件...", schema);
            FileSource<String> source = FileSource
                    .forRecordStreamFormat(new TextLineInputFormat(), new Path(filePath))
                    .build();
            CloseableIterator<String> stringCloseableIterator = env.fromSource(source, WatermarkStrategy.noWatermarks(), "sql-file-source")
                    .collectAsync();
            env.execute("Read SQL File");
            while (stringCloseableIterator.hasNext()){
                textLineList.add(stringCloseableIterator.next());
            }
            env.close();
        }
        String sqlText = String.join("\n", textLineList);
        for (String line : textLineList) {
            line = line.replaceAll("\\s+", " ");
            if (line.toUpperCase().startsWith("SET VAR:")){
                String varMap = line.replaceAll("set\\s+var:", "").replaceAll(";.*", "");
                String varKey = varMap.split("=")[0].trim();
                String varValue = varMap.split("=")[1].trim();
                sqlText = sqlText.replace("${" + varKey + "}", varValue);
                LOGGER.warn("SQL文件设置的自定义变量: {}={}", varKey, varValue);
            }
        }
        if (kv != null){
            for (String i : kv) {
                String[] split = i.split("=");
                String varKey = split[0];
                String varValue = split[1];
                sqlText = sqlText.replace("${" + varKey + "}", varValue);
                LOGGER.warn("外部参数设置的自定义变量: {}={}(若已由SQL文件设置，则该自定义变量'{}'将被忽略)", varKey, varValue, varKey);
            }
        }
        ArrayList<String> sqlStatement = new ArrayList<>(Arrays.asList(sqlText.split("\n")));
        return String.join("\n", sqlStatement).split(sqlSeparatorRegex);
    }

    /**
     *
     * @param sqlStmts
     */
    private static void execSqlStmt(String[] sqlStmts){
        // 加载jar包路径到tenv环境。
        ArrayList<String> jarsPathList = new ArrayList<>();
        for (String sql : sqlStmts) {
            sql = sql.replaceAll("--.*", "").trim();
            if (sql.toUpperCase().startsWith("ADD JAR")) {
                sql = sql.replace("'", "");
                Matcher jarPathMatcher = Pattern.compile("(?<=').*(?=')").matcher(sql);
                String jarPath = "";
                if (jarPathMatcher.find()){
                    jarPath = jarPathMatcher.group().trim();
                }
                jarsPathList.add(jarPath);
            }
        }
        if (!jarsPathList.isEmpty()){
            tenv.getConfig().set("pipeline.jars", String.join(";", jarsPathList));
        }

        StatementSet statementSet = tenv.createStatementSet();
        boolean statementSetFlag = false;
        int sqlId = 0;
        for (String sql : sqlStmts) {
            sql = sql.replaceAll("--.*", "").trim();
            if (sql.isBlank()){
                continue;
            }
            if (sql.toUpperCase().startsWith("SET")) {
                sql = sql.replace("'", "");
                Matcher key = Pattern.compile(keySetRegex).matcher(sql);
                Matcher value = Pattern.compile(valueSetRegex).matcher(sql);
                if (key.find() && value.find()){
                    String keyStr;
                    String valueStr;
                    keyStr = key.group().trim();
                    valueStr = value.group().trim();
                    if ("TABLE.SQL-DIALECT".equalsIgnoreCase(keyStr)){
                        if ("HIVE".equalsIgnoreCase(valueStr)){
                            tenv.getConfig().setSqlDialect(SqlDialect.HIVE);
                            LOGGER.warn("FLINK SQL 已切换为 \"HIVE\" 方言");
                            // System.out.println("FLINK SQL 已切换为 \"HIVE\" 方言");
                        } else if ("DEFAULT".equalsIgnoreCase(valueStr)) {
                            tenv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
                            LOGGER.warn("FLINK SQL 已切换为 \"DEFAULT\" 方言");
                            // System.out.println("FLINK SQL 已切换为 \"DEFAULT\" 方言");
                        }
                    }
                    if (keyStr.toUpperCase().startsWith("VAR:")){
                        continue;
                    }
                    tenv.getConfig().set(keyStr, valueStr);
                }
            }
            else if (sql.toUpperCase().contains("INSERT")) {
                statementSet.addInsertSql(sql);
                statementSetFlag = true;
            } else {
                sqlId += 1;
                LOGGER.warn("-------------- [SQL-{} start] -------------\n{}", sqlId, sql);
                LOGGER.warn("-------------- [SQL-{}   end] -------------", sqlId);
                tenv.executeSql(sql).print();
            }
        }
        if (statementSetFlag){
            statementSet.execute();
        }
    }

    private static String getStartString(){
        return "\n" +
                "                       _____ _ _       _      ____   ___  _     \n" +
                "   _____  _____  ___  |  ___| (_)_ __ | | __ / ___| / _ \\| |    \n" +
                "  / _ \\ \\/ / _ \\/ __| | |_  | | | '_ \\| |/ / \\___ \\| | | | |    \n" +
                " |  __/>  <  __/ (__  |  _| | | | | | |   <   ___) | |_| | |___ \n" +
                "  \\___/_/\\_\\___|\\___| |_|   |_|_|_| |_|_|\\_\\ |____/ \\__\\_\\_____|\n" +
                "                                                    "+ versionString;

    }
}
