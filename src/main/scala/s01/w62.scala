package s01

import org.apache.hadoop.fs._
import org.apache.spark.{SparkConf, SparkContext}

import java.io.IOException


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

}
