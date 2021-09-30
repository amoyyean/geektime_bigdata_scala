根据https://blog.csdn.net/wankunde/article/details/103623897
有2种不同的计算Table大小的方式
1. CommandUtils.calculateTotalSize(sparkSession, tableMeta)
2. LogicalPlanStats

还没理解这种方法计算出来的size的区别和关联。

怎么通过类似spark.sessionState.executePlan(df.queryExecution.logical).optimizedPlan.stats.sizeInBytes或spark.table(tbl).queryExecution.optimizedPlan.stats获取的128MB或其他大小文件的所需的partition数量还不是太明白。另外repartition之后产生的table文件是parquet文件，这种压缩文件对table和文件大小的计算有何影响也不太明白。

目前按照网上找到的代码片段进行一些尝试。