package com.dupeng.flink.sql;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Auther: dupeng
 * @Date: 2023/09/22/09:12
 * @Description:
 */
public class ExecSQLFile4 {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExecSQLFile4.class);
    private static final String versionString = "v20230922";
    private static final String sqlSeparatorRegex = ";\\s*$|;(?=\\s*\\n)|;(?=\\s*--)";
    private static final String keySetRegex = "(?<=[sS][eE][tT] ).*(?=\\s*\\=)";
    private static final String valueSetRegex = "(?<=\\=).*";

    public static void main(String[] args) throws Exception {
        LOGGER.info(getStartString());
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        if (!params.has("f") || params.get("f") == null) {
            throw new IllegalArgumentException("No sql file specified.");
        }
        String filePath = params.get("f");
        Collection<String> kv = params.getMultiParameter("df");
        execSqlStmt(parseSqlFile(filePath, kv));
    }

    /**
     * 使用Flink 的文件IO读取远程文件的内容，如果是本地文件则使用本地文件IO
     */
    private static String[] parseSqlFile(String filePath, Collection<String> kv) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfiguration().setString("pipeline.name", "read-sql-from-file");
        // 必须设置并行度为1，否则读取的文件是乱序！
        env.setParallelism(1);
        List<String> textLineList;
        if (new File(filePath).exists()){
            LOGGER.info("将从本地文件系统读取文件...");
            textLineList = Files.readAllLines(Paths.get(filePath));
        }
        else {
            LOGGER.info("将从远程文件系统读取文件...");
            textLineList = env.readTextFile(filePath).collect();
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
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
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
        if (jarsPathList.size() > 0){
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
                        } else if ("DEFAULT".equalsIgnoreCase(valueStr)) {
                            tenv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
                            LOGGER.warn("FLINK SQL 已切换为 \"DEFAULT\" 方言");
                        }
                    }
                    if (keyStr.toUpperCase().startsWith("VAR:")){
                        continue;
                    }
                    // 初始化config
                    // LOGGER.warn("key:{}, value:{}", keyStr, valueStr);
                    tenv.getConfig().set(keyStr, valueStr);
                }
            }
            else if (sql.toUpperCase().contains("INSERT")) {
                statementSet.addInsertSql(sql);
                statementSetFlag = true;
            } else {
                sqlId += 1;
                LOGGER.warn("[{}]-------------- SQL STATEMENT -------------\n{}", sqlId, sql);
                tenv.executeSql(sql).print();
            }
        }
        if (statementSetFlag){
            statementSet.execute();
        }
    }

    private static String getStartString(){
        return "\n" +
                "                        __ _ _       _                _ \n" +
                "   _____  _____  ___   / _| (_)_ __ | | __  ___  __ _| |\n" +
                "  / _ \\ \\/ / _ \\/ __| | |_| | | '_ \\| |/ / / __|/ _` | |\n" +
                " |  __/>  <  __/ (__  |  _| | | | | |   <  \\__ \\ (_| | |\n" +
                "  \\___/_/\\_\\___|\\___| |_| |_|_|_| |_|_|\\_\\ |___/\\__, |_|\n" +
                "                                                   |_|  \n" +
                "                                               " + versionString;

    }
}
