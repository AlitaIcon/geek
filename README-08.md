# geek
geek homework - spark
第二题目前对相关api掌握不到位，且资料较少，不能够完全按照要求实现，希望后面的课程中可以提供好的学习或资料，否则大家都不好掌握


## run
01：
src/main/scala/s01/w6.scala
```java
object w6 {
  def main(args: Array[String]): Unit = {

    val input = args(0)
    /**
     * 首先获取路径下的文件列表，unionRDD 按照wordcount来构建
     */
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //1.获取hadoop操作文件的api
    val fs = FileSystem.get(sc.hadoopConfiguration)
    //2.读取目录下的文件，并生成列表
    val filelist = fs.listFiles(new Path(input), true)
    //3.遍历文件，并读取文件类容成成rdd，结构为（文件名，单词）
    var unionrdd = sc.emptyRDD[(String,String)] // rdd声明变量为 var
    while (filelist.hasNext){
      val abs_path = new Path(filelist.next().getPath.toString)
      val file_name = abs_path.getName.slice(0, 1) //文件名称
      val rdd1 = sc.textFile(abs_path.toString).flatMap(_.split(" ").map((file_name,_)))
      //4.将遍历的多个rdd拼接成1个Rdd
      unionrdd = unionrdd.union(rdd1)
    }
    //5.构建词频（（文件名，单词），词频）
    val rdd2 = unionrdd.map(word => {(word, 1)}).reduceByKey(_ + _)
    //6.//调整输出格式,将（文件名，单词），词频）==》 （单词，（文件名，词频）） ==》 （单词，（文件名，词频））汇总
    val frdd1 = rdd2.map(word =>{(word._1._2,String.format("(%s,%s)",word._1._1,word._2.toString))})
    val frdd2 = frdd1.reduceByKey(_ +"," + _)
    val frdd3 = frdd2.map(word =>String.format("\"%s\",{%s}",word._1,word._2))
    frdd3.foreach(println)
  }

}
```

02: src/main/scala/s01/w62.scala

```java

object w62 {

  @throws[IOException]
  def main(args: Array[String]): Unit = {
    val input = args(0).replace("\\", "/")
    val output = args(1).replace("\\", "/")

//    val maxConcurrency = args(2).toInt
//    val ignoreFailure = args(3).toBoolean

    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //1.获取hadoop操作文件的api
    val fs = FileSystem.get(sc.hadoopConfiguration)
    //2.读取目录下的文件，并生成列表
    val filelist = fs.listFiles(new Path(input), true)
    while (filelist.hasNext){
      val abs_path = new Path(filelist.next().getPath.toString)
      val file_name = abs_path.getName
      val ouput_file = new Path(output, file_name)
      FileUtil.copy(
        abs_path.getFileSystem(sc.hadoopConfiguration),
        abs_path,
        ouput_file.getFileSystem(sc.hadoopConfiguration),
        ouput_file,
        false,
        sc.hadoopConfiguration
      )
    }

    }
```