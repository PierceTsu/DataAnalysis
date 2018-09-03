package com.pierce.stat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * @Project : stat
 * @Package Name : com.pierce.stat
 * @Description: PartitionerApp
 * @Author : piercetsu@gmail.com
 * @Create Date: 2018-09-03 21:44
 */
public class PartitionerApp {

    public static class PartitionerMapper extends Mapper<Object, Text, Text, LongWritable> {

        private final static LongWritable one = new LongWritable(1);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");
            context.write(new Text(words[0]), new LongWritable(Long.parseLong(words[1])));

        }
    }

    public static class MyPatitioner extends Partitioner<Text, LongWritable> {

        public int getPartition(Text key, LongWritable value, int numPartitions) {
            if (key.toString().equals("xiaomi")) {
                return 0;
            }

            if (key.toString().equals("huawei")) {
                return 1;
            }

            if (key.toString().equals("iPhone8")) {
                return 2;
            }
            return 3;
        }
    }

    public static class PartitionerReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        private LongWritable result = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (remainingArgs.length != 2) {
            System.err.println("Usage: partitioner count <in> <out>");
            System.exit(2);
        }

        //清除已存在的输出目录
        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
            System.out.println("output file exists, now is deleted");
        }

        Job job = Job.getInstance(conf, "partitioner count");
        job.setJarByClass(WordCountApp.class);

        //设置mapper相关参数
        job.setMapperClass(PartitionerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置reduce相关参数
        job.setReducerClass(PartitionerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //通过job设置combiner处理类, 与reduce相仿
        job.setCombinerClass(PartitionerReducer.class);

        //设置job的partition
        job.setPartitionerClass(MyPatitioner.class);
        //设置4个reducer, 每个分区一个
        job.setNumReduceTasks(4);

        FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));

        boolean flag = job.waitForCompletion(true);
        System.out.println("flag: " + flag);
        System.exit(flag ? 0 : 1);
    }
}
