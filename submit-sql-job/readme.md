# spark_submit_mini.py
## 使用方式
此python脚本对`spark-submit`命令进行简单封装：将传入的参数传递给
`spark-submit`，另外配合`exec_spark_sql.py`脚本，通过`-f`参数指定一个本地文件系统的SQL脚本，将这个SQL脚本的内容传递给`exec_spark_sql.py`。
这样在提交spark任务到yarn或k8s时，可以使用`cluster`模式提交，不仅仅局限于client模式。


```shell
python spark_submit_mini.py \
--master yarn \
--deploy-mode cluster \
--name spark_submit_mini \
--jars /path/to/jars \
--driver-memory 2g \
--executor-memory 2g \
--num-executors 3 \
--conf conf1=value1 \
--conf conf2=value2 \
--spark-home /path/to/spark_home \
-s /path/to/exec_spark_sql.py \
-f /path/to/some_sql.sql 
```

假设 some_sql.sql 的内容如下：
```sql
select 'hello', now();

insert into t1
select
*
from t2;
```
运行以上脚本，`spark_submit_mini.py`会将其转换为：
```shell
/path/to/spark_home/bin/spark-submit \
--conf spark.master=yarn \
--conf spark.submit.deployMode=cluster \
--conf spark.app.name=spark_submit_mini \
--conf spark.jars=/path/to/jars \
--conf spark.driver.memory=2g \
--conf spark.executor.memory=2g \
--conf spark.executor.instances=3 \
--conf conf1=value1 \
--conf conf2=value2 \
/path/to/exec_spark_sql.py \
-e \
"select 'hello', now();

insert into t1
select
*
from t2;"
```
