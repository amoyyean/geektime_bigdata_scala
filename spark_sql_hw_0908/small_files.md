# 如何避免小文件问题?

由于文件压缩，文件格式的不同，Java Heap上的文件所占空间的大小，在Spark环境里比较不容易估计产生的文件的实际大小。

1. Coalesce

Coalesce不会产生shuffle，当输出的partition数量小于输入的partition数量时会降低作业的并行度。

2. Repartition
   
Repartition会产生shuffle，不会改变原来的输入的partition数量决定的作业并行度。当输出的partition数量很低，比如1，shuffle的代价可以被高并行度补偿时，或者产生的文件数比Spark环境的partition数量更大比较适合使用Repartition。

3. Range Partition

用range范围，即partition key的min和max值，而不是根据partition key的哈希值或轮流放(RoundRobin)放入不同的文件中，把一些历史文件合并到一起。

4. Compact小文件

通过设置自动合并文件的参数，如文件个数小于多少的文件数到达一定阈值。也可以通过手动的Compact命令触发。我的判断对于已经生成的小文件手动的处理更适合Spark的环境。

类似于HBase里的Minor Compaction和Major Compaction。如果自动触发，像HBase Major Compaction那样，合并文件过多时可能会占用过多系统资源。如果写入时能自动监测到产生的小文件数量超过一定阈值，在写入时即可立即合并判断可以降低一次性合并过多文件造成的系统资源消耗峰值。

自动触发的合并规则，如合并文件数，合并文件大小，下一个加入合并队列的文件大小选择，像AQE一样自动减少reducer的个数，可以根据具体环境制定合适策略。

”HDFS小文件过多，也可以通过生成HAR文件或者Sequence File来解决“这种解决方案以前未接触，还不知道后面的原理。

5. SPARK SQL运行INSERT命令时可以加入Hive样式的Coalesce或Repartition的提示(Hint)，让用户只写SQL也能实现Coalesce或Repartition。

https://issues.apache.org/jira/browse/SPARK-24940
 
