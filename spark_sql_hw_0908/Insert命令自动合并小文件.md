# 作业3 Insert命令自动合并小文件

- 我们讲过AQE可以自动调整reducer的个数，但是正常跑Insert命令不会自动合并小文件，例如
```sql
insert into t1 select * from t2;
  ```
- 请加一条物理规则（Strategy），让Insert命令自动进行小文件合并(repartition)。（不用考虑bucket表，不用考虑Hive表）

## 代码参考

```scala
object RepartitionForInsertion extends Rule[SparkPlan] {
override def apply(plan: SparkPlan): SparkPlan = {
plan transformDown {
case i @ InsertIntoDataSourceExec(child, _, _, partitionColumns, _)
...
val newChild = ...
i.withNewChildren(newChild :: Nil)
}
}
}
```

## 自己的解答


### Logical Plan的修改方法

可以参考[MyInsertOptimizerExtension](https://github.com/amoyyean/SparkMyOptimizerExtension/blob/master/src/main/scala/com/geektime/linyan/MyInsertOptimizerExtension.scala)和[RepartitionForInsertion](https://github.com/amoyyean/SparkMyOptimizerExtension/blob/master/src/main/scala/com/geektime/linyan/RepartitionForInsertion.scala)中的代码。


### Spark Plan的修改方法

生成1个SparkMyStrategyExtension.scala文件，内容如下

```scala
package com.geektime.linyan

import org.apache.spark.sql.SparkSessionExtensions

class SparkMyStrategyExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy { session =>
      RepartitionForInsertion(session)
    }
  }
}
```

生成1个RepartitionForInsertion.scala文件，内容如下

```scala
package com.geektime.linyan

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{CoalesceExec, SparkPlan}

object RepartitionForInsertion extends Rule[SparkPlan] {
override def apply(plan: SparkPlan): SparkPlan = {
plan transformDown {
case i: DataWritingCommand =>
i.withNewChildren(CoalesceExec(1, planLater(plan)))
DataWritingCommandExec(i, planLater(plan))

}
}
}
```

--

## 助教-(张)彦功回答 2021/10/23



- 在 SqlBase.g4 增加 Compact Table 命令
- 在 SparkSqlParser.scala 增加 vistCompactTable 方法
- 在 commdand 目录下的 tables.scala (助教选择，也可以新建scala文件)增加 CompactTableCommand 类

参考文章：[overwrite](https://stackoverflow.com/questions/38487667/overwrite-specific-partitions-in-spark-dataframe-write-method)


