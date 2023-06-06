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

        // args: train test out
        JobConf conf = new JobConf(Main.class);

        Configuration meanConf = new Configuration();
        Job getMean = Job.getInstance(conf, "mean");
        getMean.setJarByClass(Main.class);
        //getMean.setInputFormatClass(FileInputFormat.class);
        getMean.setMapperClass(Mean.MeanMapper.class);
        getMean.setReducerClass(Mean.MeanReducer.class);
        getMean.setOutputKeyClass(LongWritable.class);
        getMean.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(getMean, new Path(args[0]));
        FileOutputFormat.setOutputPath(getMean, new Path("/mean-out"));
        //System.exit(getMean.waitForCompletion(true) ? 0 : 1);
        //getMean.waitForCompletion(true);


        Configuration sigmaConf = new Configuration();
        Job getSigma = Job.getInstance(conf, "sigma");
        getSigma.setJarByClass(Main.class);
        //getSigma.setInputFormatClass(FileInputFormat.class);
        getSigma.setMapperClass(Sigma.SigmaMapper.class);
        getSigma.setReducerClass(Sigma.SigmaReducer.class);
        getSigma.setOutputKeyClass(LongWritable.class);
        getSigma.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(getSigma, new Path(args[0]));
        FileOutputFormat.setOutputPath(getSigma, new Path("/sigma-out"));
        getSigma.addCacheFile(new Path("/mean-out/part-r-00000").toUri());
        //System.exit(getSigma.waitForCompletion(true) ? 0 : 1);
        //getSigma.waitForCompletion(true);


        Job knn = Job.getInstance(conf, "knn");
        knn.setJarByClass(Main.class);
        knn.setInputFormatClass(TextInputFormat.class);
        knn.setMapperClass(Knn.KnnMapper.class);
        knn.setReducerClass(Knn.KnnReducer.class);
        knn.setOutputKeyClass(LongWritable.class);
        knn.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(knn, new Path(args[0]));
        FileOutputFormat.setOutputPath(knn, new Path(args[2]));
        knn.addCacheFile(new Path(args[1]).toUri());
        knn.addCacheFile(new Path("/sigma-out/part-r-00000").toUri());
        //System.exit(knn.waitForCompletion(true) ? 0 : 1);
        //knn.waitForCompletion(true);


        int code = 1;

        ControlledJob A = new ControlledJob(conf);
        A.setJob(getMean);
        ControlledJob B = new ControlledJob(conf);
        B.setJob(getSigma);
        ControlledJob C = new ControlledJob(conf);
        C.setJob(knn);
        B.addDependingJob(A);
        C.addDependingJob(B);

        JobControl jobControl = new JobControl("ctrl");
        jobControl.addJob(A);
        jobControl.addJob(B);
        jobControl.addJob(C);
        //jobControl.run();
        Thread t = new Thread(jobControl);
        t.start();

        while (true)
            if (jobControl.allFinished()) {
                jobControl.stop();
                FileSystem fs = FileSystem.get(new Configuration());
                fs.delete(new Path("/sigma-out"), true);
                fs.delete(new Path("/mean-out"), true);

                //code = jobControl.getFailedJobList().size() == 0 ? 0 : 1;
                break;
            }
    }

}
