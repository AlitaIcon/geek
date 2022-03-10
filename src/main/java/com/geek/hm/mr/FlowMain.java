package com.geek.hm.mr;

import com.geek.hm.utils.PathUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Objects;

//import static com.geek.hm.utils.PathUtils.defaultPath;

public class FlowMain extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //启动job任务
        int run = ToolRunner.run(configuration, new FlowMain(), args);
        System.exit(run);
    }
    @Override
    public int run(String[] args) throws Exception {
        //1:创建一个job任务对象
        Job job = Job.getInstance(super.getConf(), "mapreduce_flowcount");
        //如果打包运行出错，则需要加该配置
        job.setJarByClass(FlowMain.class);
        //2:配置job任务对象(八个步骤)

        //第一步:指定文件的读取方式和读取路径
        job.setInputFormatClass(TextInputFormat.class);
        //TextInputFormat.addInputPath(job, new Path("hdfs://node01:8020/wordcount"));
//        String path = Objects.requireNonNull(FlowMain.class.getClassLoader().getResource("")).getPath();
        String path = "";
//        path = "hdfs://emr-header-1.cluster-285604:9000/home/student5/resources";
        if (args.length > 0){
            path = args[0];
            if(!path.contains("hdfs")){
                path = "file:///"+path;
            }
            System.out.println(path);
        }
        TextInputFormat.addInputPath(job, new Path(path+"/input/HTTP_20130313143750.dat"));

        //第二步:指定Map阶段的处理方式和数据类型
        job.setMapperClass(FlowMapper.class);
        //设置Map阶段K2的类型
        job.setMapOutputKeyClass(Text.class);
        //设置Map阶段V2的类型
        job.setMapOutputValueClass(FlowBean.class);

        //第三（分区），四 （排序）
        //第五步: 规约(Combiner)
        //第六步 分组

        //第七步：指定Reduce阶段的处理方式和数据类型
        job.setReducerClass(FlowReducer.class);
        //设置K3的类型
        job.setOutputKeyClass(Text.class);
        //设置V3的类型
        job.setOutputValueClass(FlowBean.class);
        //第八步: 设置输出类型
        job.setOutputFormatClass(TextOutputFormat.class);
        String outputPath = path+"/output/flow_output";
        //设置输出的路径
        if(path.contains("hdfs")){
            PathUtils.isPathExistOrDelete(outputPath, true);
        }else {
            PathUtils.outputFile(outputPath);
        }
        TextOutputFormat.setOutputPath(job, new Path(outputPath));
        //等待任务结束
        boolean bl = job.waitForCompletion(true);
        return bl ? 0:1;

    }
}
