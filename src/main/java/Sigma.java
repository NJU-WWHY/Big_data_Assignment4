import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static java.lang.Math.pow;


public class Sigma {
    static class SigmaMapper extends Mapper<Object, Text, LongWritable, Text> {
        private final HashMap<Integer, Double> means = new HashMap<>();
        @Override
        public void setup(Mapper.Context context) {
            // 利用LocalCache读取求均值Job的输出文件
            try {
                Path[] cacheFiles = context.getLocalCacheFiles();
                if (cacheFiles != null && cacheFiles.length > 0) {
                    String line;
                    try (BufferedReader joinReader = new BufferedReader(
                            new FileReader(cacheFiles[0].toString()))) {
                        while ((line = joinReader.readLine()) != null) {
                            int seq = Integer.parseInt(line.split("\\t")[0]);
                            double mean = Double.parseDouble(line.split("\\t")[1]);
                            means.put(seq, mean);  // 维度序号和相应维度上数据的均值
                        }
                    }
                }
            } catch (IOException e) {
                System.err.println("Exception reading DistributedCache:" + e);
            }
        }


        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines = value.toString().split("\\n");
            for (int i = 0; i < lines.length; ++i) {
                double[] sums = {0, 0, 0, 0};
                int j = i;
                for (; j < i + 20 && j < lines.length; ++j) {
                    String[] data = lines[j].split(",");
                    for (int k = 0; k < 4; ++k)
                        // 分组求每个维度上数据与均值之差的平方和
                        sums[k] += pow(Double.parseDouble(data[k]) - means.get(k), 2);
                }
                for (int k = 0; k < 4; ++k)
                    // 键：维度 值：“组内各条数据与均值之差的平方和 组内记录总数”
                    context.write(new LongWritable(k), new Text(sums[k] + " " + (j - i)));
                i = j;
            }
        }
    }


    static class SigmaReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int num = 0;
            double sum = 0;
            for (Text value: values) {
                String[] pair = value.toString().split(" ");
                num += Integer.parseInt(pair[1]);
                sum += Double.parseDouble(pair[0]);
            }
            // 键：维度 值：方差
            context.write(key, new Text(String.valueOf(sum / num)));
        }
    }
}
