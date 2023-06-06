import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class Mean {
    static class MeanMapper extends Mapper<Object, Text, LongWritable, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines = value.toString().split("\\n");
            for (int i = 0; i < lines.length; ++i) {
                double[] sums = {0, 0, 0, 0};
                int j = i;
                for (; j < i + 20 && j < lines.length; ++j) {
                    String[] data = lines[j].split(",");
                    for (int k = 0; k < 4; ++k)
                        sums[k] += Double.parseDouble(data[k]);
                }
                for (int k = 0; k < 4; ++k)
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
                String[] pair = value.toString().split(" ");
                num += Integer.parseInt(pair[1]);
                sum += Double.parseDouble(pair[0]);
            }
            context.write(key, new Text(String.valueOf(sum / num)));
        }
    }
}
