# spark-demo
spark demos

### 启动spark集群

cd spark-2.2.0-bin-hadoop2.7

启动master: `./sbin/start-master.sh`
  
查看master: http://localhost:8180/

### 提交作业

spark-submit --master spark://MacBook-Pro.local:7077  --class cc.examples.SparkPi ~/spark-demo/out/artifacts/spark_demo_jar/spark-demo.jar 

### idea

-Dspark.master=local