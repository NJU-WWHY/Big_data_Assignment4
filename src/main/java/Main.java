import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class Main {
    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

        // 参数列表顺序: args[0]->训练集路径 args[1]->测试集路径 args[2]->输出文件目录
        JobConf conf = new JobConf(Main.class);

        // 配置求均值的Job
        Configuration meanConf = new Configuration();
        Job getMean = Job.getInstance(conf, "mean");
        getMean.setJarByClass(Main.class);
        getMean.setMapperClass(Mean.MeanMapper.class);
        getMean.setReducerClass(Mean.MeanReducer.class);
        getMean.setOutputKeyClass(LongWritable.class);
        getMean.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(getMean, new Path(args[0]));
        FileOutputFormat.setOutputPath(getMean, new Path("/mean-out"));

        // 配置求方差的Job
        Configuration sigmaConf = new Configuration();
        Job getSigma = Job.getInstance(conf, "sigma");
        getSigma.setJarByClass(Main.class);
        getSigma.setMapperClass(Sigma.SigmaMapper.class);
        getSigma.setReducerClass(Sigma.SigmaReducer.class);
        getSigma.setOutputKeyClass(LongWritable.class);
        getSigma.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(getSigma, new Path(args[0]));
        FileOutputFormat.setOutputPath(getSigma, new Path("/sigma-out"));
        getSigma.addCacheFile(new Path("/mean-out/part-r-00000").toUri());  // 这是训练集各个维度数据的均值所在的文件

        // 配置求KNN分类结果的Job
        Job knn = Job.getInstance(conf, "knn");
        knn.setJarByClass(Main.class);
        knn.setInputFormatClass(TextInputFormat.class);
        knn.setMapperClass(Knn.KnnMapper.class);
        knn.setReducerClass(Knn.KnnReducer.class);
        knn.setOutputKeyClass(LongWritable.class);
        knn.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(knn, new Path(args[0]));
        FileOutputFormat.setOutputPath(knn, new Path(args[2]));
        knn.addCacheFile(new Path(args[1]).toUri());  // 测试集路径
        knn.addCacheFile(new Path("/sigma-out/part-r-00000").toUri());  // 这是训练集各个维度数据的方差所在的文件

        // 配置控制流
        ControlledJob A = new ControlledJob(conf);
        A.setJob(getMean);
        ControlledJob B = new ControlledJob(conf);
        B.setJob(getSigma);
        ControlledJob C = new ControlledJob(conf);
        C.setJob(knn);
        // 添加依赖关系
        // 执行顺序：求均值 求方差 KNN
        B.addDependingJob(A);
        C.addDependingJob(B);

        JobControl jobControl = new JobControl("ctrl");
        jobControl.addJob(A);
        jobControl.addJob(B);
        jobControl.addJob(C);
        // jobControl.run(); <- 这样似乎有问题
        // 开启一个新线程运行JobControl
        Thread t = new Thread(jobControl);
        t.start();

        while (true)
            if (jobControl.allFinished()) {  // 完成
                jobControl.stop(); 
                FileSystem fs = FileSystem.get(new Configuration());
                fs.delete(new Path("/sigma-out"), true);
                fs.delete(new Path("/mean-out"), true);  // 删除中间结果，只保留KNN输出目录
                break;
            }
    }

}
