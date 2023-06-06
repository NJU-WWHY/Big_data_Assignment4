import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static java.lang.Math.pow;
import static java.lang.Math.sqrt;

public class Knn {
    static class KnnMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        private final List<String> tests = new ArrayList<String>();  // 存储测试集
        private final List<Double> sigma = Arrays.asList(1., 1., 1., 1.);  // 各维数据的标准差，本分支使用普通的欧氏距离，故始终为1

        @Override
        public void setup(Mapper.Context context) {
            try {
                Path [] cacheFiles = context.getLocalCacheFiles();
                if (cacheFiles != null && cacheFiles.length > 0) {
                    String line;
                    try (BufferedReader joinReader = new BufferedReader(
                            new FileReader(cacheFiles[0].toString()))) {
                        while ((line = joinReader.readLine()) != null) {
                            tests.add(line);  // 读入测试集的每一行并添加
                        }
                    }
                }
            } catch (IOException e) {
                System.err.println("Exception reading DistributedCache:" + e);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] trainAttrs = value.toString().split(",");
            for (int i = 0; i < tests.size(); ++i) {
                // 对于训练集中的每一个样本，计算它与测试集中的每一个待分类样本之间的欧氏距离
                String[] testAttrs = tests.get(i).split(",", 4);
                double sum = 0;
                for (int j = 0; j < 4; ++j)
                    sum += pow((Double.parseDouble(trainAttrs[j]) - Double.parseDouble(testAttrs[j])) / sigma.get(j), 2);
                double euclidean = sqrt(sum);
                // 键：行号 值："1/距离 标签"，以距离的倒数作为权值，使得距离减小时权值增大
                context.write(new LongWritable(i), new Text(1. / euclidean + " " + trainAttrs[4]));
            }
        }
    }

    static class KnnReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> distances = new ArrayList<>();
            for (Text value: values)
                distances.add(value.toString());
            // 按照权值排序，权值大者在前
            distances.sort((s1, s2)->{
                double w1 = Double.parseDouble(s1.split(" ")[0]);
                double w2 = Double.parseDouble(s2.split(" ")[0]);
                return Double.compare(w2, w1);
            });
            // 初始化各个标签的权值总和
            HashMap<String, Double> hashMap = new HashMap<String, Double>(){
                {
                    put("setosa", 0.);
                    put("virginica", 0.);
                    put("versicolor", 0.);
                }
            };
            int k = 4;  // 取四个近邻，计算各个标签的总权值
            for (int i = 0; i < k && i < distances.size(); ++i) {
                String type = distances.get(i).split(" ", 2)[1];
                Double weight = Double.parseDouble(distances.get(i).split(" ", 2)[0]);
                hashMap.put(type, weight + hashMap.get(type));
            }
            // 权值最大者作为分类结果
            String result = hashMap.get("setosa") > hashMap.get("virginica") &&
                    hashMap.get("setosa") > hashMap.get("versicolor")? "setosa":
                    (hashMap.get("virginica") > hashMap.get("versicolor")? "virginica": "versicolor");
            // 键：行号 值：标签
            context.write(key, new Text(result));
        }
    }
}

