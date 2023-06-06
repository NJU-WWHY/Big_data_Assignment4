import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class Mean {  // 求每个维度数据的平均数 
    static class MeanMapper extends Mapper<Object, Text, LongWritable, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // value为整个训练集
            String[] lines = value.toString().split("\\n");  // 按行分开
            for (int i = 0; i < lines.length; ++i) {
                double[] sums = {0, 0, 0, 0};
                int j = i;
                for (; j < i + 20 && j < lines.length; ++j) {
                    String[] data = lines[j].split(",");
                    // 遍历每个维度，大约20条记录为一组，分组计算总和
                    for (int k = 0; k < 4; ++k)
                        sums[k] += Double.parseDouble(data[k]);
                }
                for (int k = 0; k < 4; ++k)
                    // 键：维度序号 值：“分组总和 组内记录数”
                    context.write(new LongWritable(k), new Text(sums[k] + " " + (j - i)));
                i = j;
            }
        }
    }


    static class MeanReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int num = 0;
            double sum = 0;
            for (Text value: values) {
                // 求所有分组数据的总和及总记录数
                String[] pair = value.toString().split(" ");
                num += Integer.parseInt(pair[1]);
                sum += Double.parseDouble(pair[0]);
            }
            // 键：维度序号 值：平均数
            context.write(key, new Text(String.valueOf(sum / num)));
        }
    }
}
