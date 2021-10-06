根据[AnalyzeTableCommand和LogicalPlanStats的说明](https://blog.csdn.net/wankunde/article/details/103623897)有2种不同的计算Table大小的方式
1. CommandUtils.calculateTotalSize(sparkSession, tableMeta)
2. LogicalPlanStats

经过源码阅读和网上找到的信息，理解为fileCompressionFactor的默认值为1，不另外设其他值的情况下的Analyze Table Command中调用的CommandUtils.calculateTotalSize的命令和LogicalPlanStats后面通过overide computeStats()方法计算的Stats(Statistics 类)最后就都是调用FileStatus.getLen()。

spark.sessionState.executePlan(df.queryExecution.logical).optimizedPlan.stats.sizeInBytes或spark.table(tbl).queryExecution.optimizedPlan.stats获取的的文件大小和spark-sql中运行DESC EXTENDED xxtable;的结果一致。

本地并行数受到线程数的影响，自己产生的数据表初始文件过小，而repartition命令产生的parquet文件估计每个都需要存储特定的受到列信息影响的元信息，尝试中没有产生小于512KB的文件。因此COMPACT TABLE命令没有into fileNum FILES参数时的自动分配的文件大小不能过小，否则无法稳定地产生预设的文件大小的文件。在本地调试中，暂时先设成1024B。以后有机会生成较大的表文件时再做修改和尝试。

修改的程序文件可以参考[CompactTableCommand](CompactTableCommand.scala)，[SparkSqlParser.scala](SparkSqlParser.scala)和[SqlBase.g4](SqlBase.g4)，最新2个版本的运行和结果如下
---
![compactTableSQLCommandandResult3_1](compactTableSQLCommandandResult3_1.png)
---
![compactTableSQLCommandandResult3_2](compactTableSQLCommandandResult3_2.png)
---
其他的运行和结果如下
![compactTableSQLCommandandResult](compactTableSQLCommandandResult1.png)

![compactTableSQLCommandandResult2](compactTableSQLCommandandResult2.png)
