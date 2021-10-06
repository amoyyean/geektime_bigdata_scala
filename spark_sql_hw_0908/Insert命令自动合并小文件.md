### Logical Plan的修改方法

可以参考[MyInsertOptimizerExtension](https://github.com/amoyyean/SparkMyOptimizerExtension/blob/master/src/main/scala/com/geektime/linyan/MyInsertOptimizerExtension.scala)和[RepartitionForInsertion](https://github.com/amoyyean/SparkMyOptimizerExtension/blob/master/src/main/scala/com/geektime/linyan/RepartitionForInsertion.scala)中的代码。


### Spark Plan的修改方法

生成1个SparkMyStrategyExtension.scala文件，内容如下

package com.geektime.linyan

import org.apache.spark.sql.SparkSessionExtensions

class SparkMyStrategyExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy { session =>
      RepartitionForInsertion(session)
    }
  }
}

生成1个RepartitionForInsertion.scala文件，内容如下

package com.geektime.linyan

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{CoalesceExec, SparkPlan}

object RepartitionForInsertion extends Rule[SparkPlan] {
override def apply(plan: SparkPlan): SparkPlan = {
plan transformDown {
case i: insertdatasource的Spark Plan的对象名称待确认
i.withNewChildren(CoalesceExec(1, plan))
}
}
}
