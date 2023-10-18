package com.shutantech.flink.tools;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Auther: dupeng
 * @Date: 2023/03/18/09:12
 * @Description:
 */
public class ExecSQLFile {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExecSQLFile.class);
    
    public static void main(String[] args) throws Exception {
        LOGGER.info(getLog());
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        if (!params.has("f") || params.get("f") == null) {
            throw new IllegalArgumentException("No sql file specified.");
        }
        String filePath = params.get("f");
        Collection<String> kv = params.getMultiParameter("df");
        HashMap<String, String> kvMap = new HashMap<>();
        if (params.has("df") && !kv.isEmpty()){
            for (String i : kv) {
                String[] split = i.split("=");
                kvMap.put(split[0], split[1]);
            }
            execSqlStmt(parseSqlFile(filePath, kvMap));
        }
        else execSqlStmt(parseSqlFile(filePath));
    }

    /**
     * 使用Flink 的文件IO读取远程文件的内容，如果是本地文件则使用本地文件IO
     */
    private static String[] parseSqlFile(String filePath) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 必须设置并行度为1，否则读取的文件是乱序！
        env.setParallelism(1);
        List<String> collect;
        if (filePath.startsWith("file://") || filePath.startsWith("/")){
            collect = Files.readAllLines(Paths.get(filePath));
        }
        else {
            collect = env.readTextFile(filePath).collect();
        }
        ArrayList<String> sqlStatement = new ArrayList<>();
        for (String line : collect) {
            if (! line.startsWith("--")){
                sqlStatement.add(line);
            }
        }
        return String.join("\n", sqlStatement).split(";\\s*$|;(?=\\s*\\n)");
    }

    private static String[] parseSqlFile(String filePath, Map<String, String> kvMap) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 必须设置并行度为1，否则读取的文件是乱序！
        env.setParallelism(1);
        String sqlText = String.join("\n", env.readTextFile(filePath).collect());
        if (!kvMap.isEmpty()){
            for (String key : kvMap.keySet()) {
                String value = kvMap.get(key);
                sqlText = sqlText.replace("${" + key + "}", value);
            }
        }
        ArrayList<String> sqlStatement = new ArrayList<>();
        for (String line : sqlText.split("\n")) {
            if (! line.startsWith("--")){
                sqlStatement.add(line);
            }
        }
        return String.join("\n", sqlStatement).split(";\\s*$|;(?=\\s*\\n)");
    }

    private static void execSqlStmt(String[] sqlStmt){
        Configuration config = new Configuration();
        ArrayList<String> jarsPathList = new ArrayList<>();
        for (String sql : sqlStmt) {
            sql = sql.trim();
            if (sql.toUpperCase().startsWith("SET")){
                sql = sql.replace("'", "");
                Matcher key = Pattern.compile("(?<=[sS][eE][tT] ).*(?=\\s*\\=)").matcher(sql);
                Matcher value = Pattern.compile("(?<=\\=).*").matcher(sql);
                String setKey = "";
                String setValue = "";
                if (key.find()){
                    setKey = key.group().trim();
                }
                if (value.find()){
                    setValue = value.group().trim();
                }
                config.setString(setKey, setValue);
            } else if (sql.toUpperCase().startsWith("ADD JAR")) {
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
            config.setString("pipeline.jars", String.join(";", jarsPathList));
        }
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode()
                .withConfiguration(config)
                .build();
        TableEnvironment tenv = TableEnvironment.create(settings);

        StatementSet statementSet = tenv.createStatementSet();
        boolean statementSetFlag = false;
        for (String sql : sqlStmt) {
            sql = sql.trim();
            LOGGER.warn("-------------- SQL STATEMENT -------------\n{}", sql);
            if ("".equals(sql) || sql.toUpperCase().startsWith("SET") || sql.toUpperCase().startsWith("ADD")) {
                continue;
            }
            else if (sql.toUpperCase().contains("INSERT")) {
                statementSet.addInsertSql(sql);
                statementSetFlag = true;
            } else {
                tenv.executeSql(sql).print();
            }
        }
        if (statementSetFlag){
            statementSet.execute();
        }
    }

    private static String getLog(){
        return "\n" +
                "                                _ \n" +
                "  _____  _____  ___   ___  __ _| |\n" +
                " / _ \\ \\/ / _ \\/ __| / __|/ _` | |\n" +
                "|  __/>  <  __/ (__  \\__ \\ (_| | |\n" +
                " \\___/_/\\_\\___|\\___| |___/\\__, |_|\n" +
                "                             |_|  \n";
    }
}
