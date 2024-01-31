"""
执行 spark SQL，获取结果
"""

from pyspark.sql import SparkSession

# spark = SparkSession.builder.getOrCreate()

sql = """
select * from values(1,2),(1,3),(1,4) as data(id, name)
"""
# spark.sql(sql).collect()

kv = ('a','hello')
line = "select now(), '${a}', '-- de';"
print('${' + f'{kv[0]}' + '}')

line = line.replace('${' + f'{kv[0]}' + '}', kv[1])
print(line)




