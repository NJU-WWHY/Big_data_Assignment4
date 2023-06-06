import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class Main {
    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        Job knn = Job.getInstance();
        knn.setJarByClass(Knn.class);
        knn.setInputFormatClass(TextInputFormat.class);
        knn.setMapperClass(Knn.KnnMapper.class);
        knn.setReducerClass(Knn.KnnReducer.class);
        knn.setOutputKeyClass(LongWritable.class);
        knn.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(knn, new Path(args[0]));
        FileOutputFormat.setOutputPath(knn, new Path(args[2]));
        knn.addCacheFile(new Path(args[1]).toUri());
        System.exit(knn.waitForCompletion(true) ? 0 : 1);
    }
}

