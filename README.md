# spark-encoders-problem

This problem was discovered and reproduced so far only in the Databricks cluster, and we were unable to reproduce the problem locally.

Databricks runtime version: 7.3 LTS (includes Apache Spark 3.0.1, Scala 2.12)

## Build

`sbt clean publishLocal`


## Sample Notebook Code

```scala
import org.apache.spark.sql.functions.{ col, struct, lit, udf }
import org.apache.spark.sql.{ Column, Dataset, Encoder, Encoders, SparkSession }
import org.apache.spark.sql.catalyst.encoders.{ encoderFor, ExpressionEncoder }

implicit val iSpark = spark

case class TestClass(id: Int, name: String)

val dataset: Dataset[TestClass] = spark.createDataset(Seq(TestClass(1, "some name 01")))

implicit def tuple2Encoder[A: Encoder, B: Encoder]: Encoder[(A, B)] =
  ExpressionEncoder.tuple(encoderFor[A], encoderFor[B])

import sparkEncoders.problem._

dataset.withColumnDataset[String](lit("some extra column")).show
```

The unexpected error is the following:

```
java.lang.NoSuchMethodError: org.apache.spark.sql.catalyst.encoders.package$.encoderFor(Lorg/apache/spark/sql/Encoder;)Lorg/apache/spark/sql/catalyst/encoders/ExpressionEncoder;
at sparkEncoders.problem$DatasetOps.withColumnDataset(problem.scala:26)
	at lined2b3cc361ca24eb0a4a1d5548c16399063.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(command-1435041914124373:6)
	at lined2b3cc361ca24eb0a4a1d5548c16399063.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(command-1435041914124373:111)
	at lined2b3cc361ca24eb0a4a1d5548c16399063.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(command-1435041914124373:113)
	at lined2b3cc361ca24eb0a4a1d5548c16399063.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(command-1435041914124373:115)
	at lined2b3cc361ca24eb0a4a1d5548c16399063.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(command-1435041914124373:117)
	at lined2b3cc361ca24eb0a4a1d5548c16399063.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(command-1435041914124373:119)
	at lined2b3cc361ca24eb0a4a1d5548c16399063.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(command-1435041914124373:121)
	at lined2b3cc361ca24eb0a4a1d5548c16399063.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(command-1435041914124373:123)
	at lined2b3cc361ca24eb0a4a1d5548c16399063.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(command-1435041914124373:125)
	at lined2b3cc361ca24eb0a4a1d5548c16399063.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(command-1435041914124373:127)
	at lined2b3cc361ca24eb0a4a1d5548c16399063.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(command-1435041914124373:129)
	at lined2b3cc361ca24eb0a4a1d5548c16399063.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(command-1435041914124373:131)
	at lined2b3cc361ca24eb0a4a1d5548c16399063.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(command-1435041914124373:133)
	at lined2b3cc361ca24eb0a4a1d5548c16399063.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(command-1435041914124373:135)
	at lined2b3cc361ca24eb0a4a1d5548c16399063.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(command-1435041914124373:137)
	at lined2b3cc361ca24eb0a4a1d5548c16399063.$read$$iw$$iw$$iw$$iw$$iw$$iw.<init>(command-1435041914124373:139)
	at lined2b3cc361ca24eb0a4a1d5548c16399063.$read$$iw$$iw$$iw$$iw$$iw.<init>(command-1435041914124373:141)
	at lined2b3cc361ca24eb0a4a1d5548c16399063.$read$$iw$$iw$$iw$$iw.<init>(command-1435041914124373:143)
	at lined2b3cc361ca24eb0a4a1d5548c16399063.$read$$iw$$iw$$iw.<init>(command-1435041914124373:145)
	at lined2b3cc361ca24eb0a4a1d5548c16399063.$read$$iw$$iw.<init>(command-1435041914124373:147)
	at lined2b3cc361ca24eb0a4a1d5548c16399063.$read$$iw.<init>(command-1435041914124373:149)
	at lined2b3cc361ca24eb0a4a1d5548c16399063.$read.<init>(command-1435041914124373:151)
	at lined2b3cc361ca24eb0a4a1d5548c16399063.$read$.<init>(command-1435041914124373:155)
	at lined2b3cc361ca24eb0a4a1d5548c16399063.$read$.<clinit>(command-1435041914124373)
	at lined2b3cc361ca24eb0a4a1d5548c16399063.$eval$.$print$lzycompute(<notebook>:7)
	at lined2b3cc361ca24eb0a4a1d5548c16399063.$eval$.$print(<notebook>:6)
	at lined2b3cc361ca24eb0a4a1d5548c16399063.$eval.$print(<notebook>)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at scala.tools.nsc.interpreter.IMain$ReadEvalPrint.call(IMain.scala:745)
	at scala.tools.nsc.interpreter.IMain$Request.loadAndRun(IMain.scala:1021)
	at scala.tools.nsc.interpreter.IMain.$anonfun$interpret$1(IMain.scala:574)
	at scala.reflect.internal.util.ScalaClassLoader.asContext(ScalaClassLoader.scala:41)
	at scala.reflect.internal.util.ScalaClassLoader.asContext$(ScalaClassLoader.scala:37)
	at scala.reflect.internal.util.AbstractFileClassLoader.asContext(AbstractFileClassLoader.scala:41)
	at scala.tools.nsc.interpreter.IMain.loadAndRunReq$1(IMain.scala:573)
	at scala.tools.nsc.interpreter.IMain.interpret(IMain.scala:600)
	at scala.tools.nsc.interpreter.IMain.interpret(IMain.scala:570)
	at com.databricks.backend.daemon.driver.DriverILoop.execute(DriverILoop.scala:219)
	at com.databricks.backend.daemon.driver.ScalaDriverLocal.$anonfun$repl$1(ScalaDriverLocal.scala:204)
	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
	at com.databricks.backend.daemon.driver.DriverLocal$TrapExitInternal$.trapExit(DriverLocal.scala:789)
	at com.databricks.backend.daemon.driver.DriverLocal$TrapExit$.apply(DriverLocal.scala:742)
	at com.databricks.backend.daemon.driver.ScalaDriverLocal.repl(ScalaDriverLocal.scala:204)
	at com.databricks.backend.daemon.driver.DriverLocal.$anonfun$execute$10(DriverLocal.scala:431)
	at com.databricks.logging.UsageLogging.$anonfun$withAttributionContext$1(UsageLogging.scala:239)
	at scala.util.DynamicVariable.withValue(DynamicVariable.scala:62)
	at com.databricks.logging.UsageLogging.withAttributionContext(UsageLogging.scala:234)
	at com.databricks.logging.UsageLogging.withAttributionContext$(UsageLogging.scala:231)
	at com.databricks.backend.daemon.driver.DriverLocal.withAttributionContext(DriverLocal.scala:48)
	at com.databricks.logging.UsageLogging.withAttributionTags(UsageLogging.scala:276)
	at com.databricks.logging.UsageLogging.withAttributionTags$(UsageLogging.scala:269)
	at com.databricks.backend.daemon.driver.DriverLocal.withAttributionTags(DriverLocal.scala:48)
	at com.databricks.backend.daemon.driver.DriverLocal.execute(DriverLocal.scala:408)
	at com.databricks.backend.daemon.driver.DriverWrapper.$anonfun$tryExecutingCommand$1(DriverWrapper.scala:653)
	at scala.util.Try$.apply(Try.scala:213)
	at com.databricks.backend.daemon.driver.DriverWrapper.tryExecutingCommand(DriverWrapper.scala:645)
	at com.databricks.backend.daemon.driver.DriverWrapper.getCommandOutputAndError(DriverWrapper.scala:486)
	at com.databricks.backend.daemon.driver.DriverWrapper.executeCommand(DriverWrapper.scala:598)
	at com.databricks.backend.daemon.driver.DriverWrapper.runInnerLoop(DriverWrapper.scala:391)
	at com.databricks.backend.daemon.driver.DriverWrapper.runInner(DriverWrapper.scala:337)
	at com.databricks.backend.daemon.driver.DriverWrapper.run(DriverWrapper.scala:219)
	at java.lang.Thread.run(Thread.java:748)
```
