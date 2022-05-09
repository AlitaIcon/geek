# geek
geek homework - spark


## 作业1：为 Spark SQL 添加一条自定义命令

* SHOW VERSION；
* 显示当前 Spark 版本和 Java 版本。

步骤：
1. 增加DSL
   编辑catalyst工程下的parser/SqlBase.g4文件
```antlr4 
VERSION: 'VERSION';

statement:
....
| SHOW VERSION                                                     #showVersion
.....
```
2. 编译DSL，生成scala代码
```bash
mvn antlr4:antlr4
```
生成的stub代码在catalyst/parser/SqlBaseParser.java, SqlBaseVisitor.java中。


3. 实现ShowVersion的执行命令
   spark-sql模块
```scala
case class ShowVersionCommand() extends LeafRunnableCommand{
  override val output: Seq[Attribute] = Seq(AttributeReference("version", StringType)())
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sparkVersion = sparkSession.version;
    val scalaVersion = util.Properties.versionNumberString
    val output = "Spark Version:%s, scala:%s".format(sparkVersion, scalaVersion)
    Seq(Row(output))
  }
}

// 在SparkSqlParser中添加 ShowVersion的命令实现
override def visitShowVersion(ctx: ShowVersionContext): LogicalPlan = withOrigin(ctx) {

  ShowVersionCommand()

}
```

[运行结果](https://gitee.com/asiwen/geek-bigdata-camp/blob/master/week9_assigment/show_version_result.png)


## 作业2 构造满足要求的SQL.

### 1.构建一条 SQL，同时 apply 下面三条优化规则
* CombineFilters
* CollapseProject
* BooleanSimplification

启动 spark-shell 编写如下代码
```scala
spark.sql("SET spark.sql.planChangeLog.level=WARN")

val df_people = spark.read.option("delimiter", ";").option("header",true).csv(path+"/people.csv")
df_peopel.createOrReplaceTempView("people")

//spark-sql中简洁方法：
//create table people(name string, age int, job string) using csv options(path '/<absolute_path>people.csv', delimiter ';', header true);

spark.sql("select name from (select * from people where job='Developer') a where age=30 and 1=1").show()

```

优化器执行结果：
```bash
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.PushDownPredicates ===
 GlobalLimit 21                                                 GlobalLimit 21
 +- LocalLimit 21                                               +- LocalLimit 21
    +- Project [cast(name#116 as string) AS name#257]              +- Project [cast(name#116 as string) AS name#257]
       +- Project [name#116]                                          +- Project [name#116]
!         +- Filter ((cast(age#117 as int) = 30) AND (1 = 1))            +- Project [name#116, age#117, job#118]
!            +- Project [name#116, age#117, job#118]                        +- Filter ((job#118 = Developer) AND ((cast(age#117 as int) = 30) AND (1 = 1)))
!               +- Filter (job#118 = Developer)                                +- Relation[name#116,age#117,job#118] csv
!                  +- Relation[name#116,age#117,job#118] csv  
22/05/07 01:28:10 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ColumnPruning ===
 GlobalLimit 21                                                                                GlobalLimit 21
 +- LocalLimit 21                                                                              +- LocalLimit 21
    +- Project [cast(name#116 as string) AS name#257]                                             +- Project [cast(name#116 as string) AS name#257]
       +- Project [name#116]                                                                         +- Project [name#116]
!         +- Project [name#116, age#117, job#118]                                                       +- Project [name#116]
             +- Filter ((job#118 = Developer) AND ((cast(age#117 as int) = 30) AND (1 = 1)))               +- Filter ((job#118 = Developer) AND ((cast(age#117 as int) = 30) AND (1 = 1)))
                +- Relation[name#116,age#117,job#118] csv                                                     +- Relation[name#116,age#117,job#118] csv
           
22/05/07 01:28:10 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.CollapseProject ===
 GlobalLimit 21                                                                                GlobalLimit 21
 +- LocalLimit 21                                                                              +- LocalLimit 21
    +- Project [cast(name#116 as string) AS name#257]                                             +- Project [cast(name#116 as string) AS name#257]
!      +- Project [name#116]                                                                         +- Filter ((job#118 = Developer) AND ((cast(age#117 as int) = 30) AND (1 = 1)))
!         +- Project [name#116]                                                                         +- Relation[name#116,age#117,job#118] csv
!            +- Filter ((job#118 = Developer) AND ((cast(age#117 as int) = 30) AND (1 = 1)))   
!               +- Relation[name#116,age#117,job#118] csv                                      
           
22/05/07 01:28:10 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ConstantFolding ===
 GlobalLimit 21                                                                          GlobalLimit 21
 +- LocalLimit 21                                                                        +- LocalLimit 21
    +- Project [cast(name#116 as string) AS name#257]                                       +- Project [cast(name#116 as string) AS name#257]
!      +- Filter ((job#118 = Developer) AND ((cast(age#117 as int) = 30) AND (1 = 1)))         +- Filter ((job#118 = Developer) AND ((cast(age#117 as int) = 30) AND true))
          +- Relation[name#116,age#117,job#118] csv                                               +- Relation[name#116,age#117,job#118] csv
           
22/05/07 01:28:10 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.BooleanSimplification ===
 GlobalLimit 21                                                                       GlobalLimit 21
 +- LocalLimit 21                                                                     +- LocalLimit 21
    +- Project [cast(name#116 as string) AS name#257]                                    +- Project [cast(name#116 as string) AS name#257]
!      +- Filter ((job#118 = Developer) AND ((cast(age#117 as int) = 30) AND true))         +- Filter ((job#118 = Developer) AND (cast(age#117 as int) = 30))
          +- Relation[name#116,age#117,job#118] csv                                            +- Relation[name#116,age#117,job#118] csv

```
* PushDownPredicates三条子规则之一为CombineFilters
```scala
object PushDownPredicates extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsAnyPattern(FILTER, JOIN)) {
    CombineFilters.applyLocally
      .orElse(PushPredicateThroughNonJoin.applyLocally)
      .orElse(PushPredicateThroughJoin.applyLocally)
  }
}
```

### 2. 构建一条 SQL，同时 apply 下面五条优化规则
1. ConstantFolding
2. PushDownPredicates
3. ReplaceDistinctWithAggregate
4. ReplaceExceptWithAntiJoin
5. FoldablePropagation

```scala
val df_salary = spark.read.json(path+"/salary.json")
df_salary.createOrReplaceTempView("salary")
```
构造sql语句：
```sql
select distinct(name), age from 
  (select * from people where job='Developer') a 
where age>=30 and 1=1  

except 

select name, 32 as age from salary
```
* 外层where条件中1=1用到ConstantFolding。
* 两个where条件用到PushDownPredicates
* distinct用到ReplaceDistinctWithAggregate规则
* except用到ReplaceExceptWithAntiJoin
* 常量列用到FoldablePropagation

## 作业三
实现自定义优化规则
```scala
package cn.tranq

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

case class MyPushDown (spark: SparkSession) extends Rule[LogicalPlan] with Logging {
  def apply(plan: LogicalPlan): LogicalPlan = {

    logWarning("apply MyPushDown")

    plan
  }
}

class MySparkSessionExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule { session =>
       MyPushDown(session)
    }
  }
}
```
bin/spark-sql --jars spark-sql_2.12-ext.jar --conf spark.sql.extensions=cn.tranq.MySparkSessionExtension

在spark-sql中执行， show tables或者select语句均会打印apply MyPushDown日志。表明扩展的优化规则被执行到了。

[运行结果](https://gitee.com/asiwen/geek-bigdata-camp/blob/master/week9_assigment/mypushdown_result.png)

注意：
* logWarning 日志与整个spark-sql（即spark app）有关。
* rule能否运行到与规则的匹配规则有关（后续深入验证一下）
