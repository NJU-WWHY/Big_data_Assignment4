import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;


import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList; // 引入 ArrayList 类
import java.util.Comparator;
import java.util.Hashtable;
import java.util.PriorityQueue;


public class KNN {
    //邻居的数目
    final static int nkr_num = 4;

    final static String setosa = "setosa";
    final static String versicolor = "versicolor";
    final static String virginica = "virginica";

    //是否带有权重
    final static boolean is_weight = true;
    final static int[] k_weights = {1,1, 1, 1};

    public static double sim_distance(ArrayList<Double> vector1, ArrayList<Double> vector2) {
        double distance = 0;

        if (vector1.size() == vector2.size()) {
            for (int i = 0; i < vector1.size(); i++) {
                double temp = Math.pow((vector1.get(i) - vector2.get(i)), 2);
                distance += temp;
            }
            distance = Math.sqrt(distance);
        }
        return distance;
    }

    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

        //设置conf全局变量来共享测试集
        Configuration conf = new Configuration();
        conf.set("test_file",args[2]);
        Job job = Job.getInstance(conf, "WWHY-KNN");

        job.setJarByClass(KNN.class);

        //设置Mapper,Combiner,Reducer
        job.setMapperClass(KNNMapper.class);
        job.setReducerClass(KNNReducer.class);

        //设置Context格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        //设置输入与输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //等待执行并退出
        boolean flag = job.waitForCompletion(true);
        if (flag) {
            System.out.println("Ok");
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class KNNMapper extends Mapper<Object, Text,
            IntWritable, Text> {
        //获取测试集的文件路径
        String test_file_path;
        protected void setup(Context context) throws IOException, InterruptedException {//从全局配置获取配置参数
                    Configuration conf = context.getConfiguration();
            test_file_path = conf.get("test_file"); //这样就拿到了.
        }
        //String test_file_path = "hdfs://localhost:9000/lab4-testfile/iris_test.csv";
        //这个数组存放本节点的训练集块
        ArrayList<Sample> test_data = new ArrayList<>();

        IntWritable key_nodeID = new IntWritable();
        Text nbr = new Text();

        Sample train_sample = new Sample();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //1
            //解析value
            String[] one_item = value.toString().split(",");
            double x0 = Double.valueOf(one_item[0]);
            double x1 = Double.valueOf(one_item[1]);
            double x2 = Double.valueOf(one_item[2]);
            double x3 = Double.valueOf(one_item[3]);
            String label = one_item[4];

            train_sample.setFeatures(x0, x1, x2, x3);
            train_sample.setCategority(label);

            //2
            //开始循环读取测试集，存入test_data的list块中
            int test_mode_id = 0;
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            Path file = new Path(test_file_path);
            FSDataInputStream inStream = fs.open(file);

            BufferedReader br = null;
            String line;
            br = new BufferedReader(new InputStreamReader(inStream));
            while ((line = br.readLine()) != null) {
                String[] features = line.split(",");
                double t0 = Double.valueOf(features[0]);
                double t1 = Double.valueOf(features[1]);
                double t2 = Double.valueOf(features[2]);
                double t3 = Double.valueOf(features[3]);

                Sample temp_sample = new Sample();

                temp_sample.setFeatures(t0, t1, t2, t3);
                temp_sample.set_test_Index(test_mode_id);

                test_data.add(temp_sample);

                test_mode_id++;
            }
            inStream.close();


            //3
            //debug专用
            //if (test_data.size() == 30) {
            ///  throw new IOException("test data size !=30  " + value.toString());
            //}

            for (int test_index = 0; test_index < test_data.size(); test_index++) {
                //计算距离并且记下距离
                double dis = KNN.sim_distance(test_data.get(test_index).getFeatures(), train_sample.getFeatures());

                String str_nbr_list = String.valueOf(dis) + "," + train_sample.getCategority();

                key_nodeID.set(test_data.get(test_index).getTest_samle_index());
                nbr.set(str_nbr_list);

                //context格式 [ test_id \t <dis, categority> ]
                context.write(key_nodeID, nbr);
            }
        }
    }

    public static class KNNReducer extends Reducer<IntWritable, Text, Text, Text> {
        final Text key_text = new Text();
        final Text val_text = new Text();

        PriorityQueue<NbrDistance> maxHeap = new PriorityQueue<NbrDistance>(11, new Comparator<NbrDistance>() {
            @Override
            public int compare(NbrDistance i1, NbrDistance i2) {
                return (i2.distance - i1.distance) > 0.0 ? 1 : 0;
            }
        });

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


            //得到测试集中同一序号到训练集中所有点的距离和类别，全部放入堆里面，去前k大
            for (Text t : values) {
                String[] nbr = t.toString().split(",");

                NbrDistance temp_nbr = new NbrDistance();
                temp_nbr.distance = Double.valueOf(nbr[0]);
                temp_nbr.categority = nbr[1];

                if (maxHeap.size() < nkr_num) {
                    maxHeap.add(temp_nbr);
                } else if (temp_nbr.distance < maxHeap.peek().distance) {
                    maxHeap.poll();
                    maxHeap.add(temp_nbr);
                }
            }


            //统计k大小的堆,得出最后的类别
            double setosa_num = 0;
            double versicolor_num = 0;
            double virginica_num = 0;

            //带权重
            if (KNN.is_weight) {
                int weight_index=0;
                while (maxHeap.size() != 0) {
                    if (maxHeap.peek().categority.equals(setosa))
                        setosa_num +=k_weights[weight_index];
                    else if (maxHeap.peek().categority.equals(versicolor))
                        versicolor_num +=k_weights[weight_index];
                    else if (maxHeap.peek().categority.equals(virginica))
                        virginica_num +=k_weights[weight_index];
                    maxHeap.poll();
                    weight_index++;
                }
            }
            //不带权重
            else {
                while (maxHeap.size() != 0) {
                    if (maxHeap.peek().categority.equals(setosa))
                        setosa_num +=1;
                    else if (maxHeap.peek().categority.equals(versicolor))
                        versicolor_num +=1;
                    else if (maxHeap.peek().categority.equals(virginica))
                        virginica_num +=1;
                    maxHeap.poll();
                }
            }

            //比较哪一类的权重最大
            Text val_cate = new Text();
            if (setosa_num > versicolor_num && setosa_num > virginica_num)
                val_cate.set(setosa);
            if (versicolor_num > setosa_num && versicolor_num > virginica_num)
                val_cate.set(versicolor);
            if (virginica_num > versicolor_num && virginica_num > setosa_num)
                val_cate.set(virginica);


            //输出结果
            Text key_id = new Text();
            key_id.set(String.valueOf(key.get()));
            context.write(key_id, val_cate);

        }
    }
}