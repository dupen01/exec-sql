package com.shutantech.flink.tools;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
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
 * @Date: 2023/03/18/09:12
 * @Description: 经测试：使用yarn-application模式提交的作业，sql文件内的set语句，
 * 如set 'taskmanager.memory.managed.size' = '0mb'配置未生效。而在yarn-per-job模式下可以生效，
 * 然而Flink官方已经计划废弃yarn-per-job模式，所以要想使用此程序来执行外部的sql文件，
 * 那么我推荐不要在sql文件内对执行环境进行配置，而在提交命令中配置。
 */
public class ExecSQLFile2 {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExecSQLFile2.class);

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
        if (new File(filePath).exists()){
            LOGGER.info("将从本地文件系统读取文件...");
            collect = Files.readAllLines(Paths.get(filePath));
        }
        else {
            LOGGER.info("将从远程文件系统读取文件...");
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
        List<String> collect;

        if (new File(filePath).exists()){
            LOGGER.info("将从本地文件系统读取文件...");
            collect = Files.readAllLines(Paths.get(filePath));
        }
        else {
            LOGGER.info("将从远程文件系统读取文件...");
            collect = env.readTextFile(filePath).collect();
        }
        String sqlText = String.join("\n", collect);
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
            if (sql.toUpperCase().startsWith("ADD JAR")) {
                sql = sql.replace("'", "");
                Matcher jarPathMatcher = Pattern.compile("(?<=').*(?=')").matcher(sql);
                String jarPath = "";
                if (jarPathMatcher.find()){
                    jarPath = jarPathMatcher.group().trim();
                }
                jarsPathList.add(jarPath);
            } else if (sql.toUpperCase().startsWith("SET")) {
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
                // 初始化config
                config.setString(setKey, setValue);
                // LOGGER.warn("key:{}, value:{}", setKey, setValue);
            }
        }
        if (jarsPathList.size() > 0){
            config.setString("pipeline.jars", String.join(";", jarsPathList));
        }

        // 根据config创建tenv执行环境
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
                "                        __ _ _       _                _ \n" +
                "   _____  _____  ___   / _| (_)_ __ | | __  ___  __ _| |\n" +
                "  / _ \\ \\/ / _ \\/ __| | |_| | | '_ \\| |/ / / __|/ _` | |\n" +
                " |  __/>  <  __/ (__  |  _| | | | | |   <  \\__ \\ (_| | |\n" +
                "  \\___/_/\\_\\___|\\___| |_| |_|_|_| |_|_|\\_\\ |___/\\__, |_|\n" +
                "                                                   |_|  \n";

    }
}
