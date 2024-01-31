#!/usr/bin/bash
python3 /root/spark/spark_sql_submit.py \
--master k8s://https://lb.kubesphere.local:6443 \
--deploy-mode cluster \
-S /opt/spark-3.5.0-bin-hadoop3 \
-x s3a://spark/exec_spark_sql.py \
--conf spark.hadoop.fs.s3a.access.key=minio123 \
--conf spark.hadoop.fs.s3a.secret.key=12345678 \
--conf spark.hadoop.fs.s3a.endpoint=http://minio.minio.svc \
--conf spark.hadoop.fs.s3a.path.style.access=true \
--conf spark.kubernetes.namespace=spark \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
--conf spark.kubernetes.container.image.pullSecrets=aliyun-registry \
--conf spark.kubernetes.container.image=registry.cn-chengdu.aliyuncs.com/duperl/spark:3.5.0_py  \
--conf spark.kubernetes.driver.podTemplateFile=/root/spark/spark-driver-template.yaml \
--conf spark.kubernetes.driver.request.cores=0.2 \
--conf spark.kubernetes.executor.request.cores=0.2 \
--conf spark.sql.session.timeZone=Asia/Shanghai \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=s3a://spark/logs \
$@