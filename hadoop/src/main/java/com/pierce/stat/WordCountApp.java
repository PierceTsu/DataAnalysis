package com.pierce.stat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;

/**
 * @Project : wordcount
 * @Package Name : com.pierce.stat
 * @Description: WordCountApp
 * @Author : piercetsu@gmail.com
 * @Create Date: 2018-09-03
 */
public class WordCountApp {

    /**
     * hadoop fs -put /user/hadoop/test.txt
     * hadoop jar wordcount.jar com.pierce.stat.WordCountApp test.txt wordcount
     * hadoop fs -cat /user/hadoop/wordcount/part-r-00000
     * @param args
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (remainingArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        //清除已存在的输出目录
        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
            System.out.println("output file exists, now is deleted");
        }

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCountApp.class);

        //设置mapper相关参数
        job.setMapperClass(WordCountJob.WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reduce相关参数
        job.setReducerClass(WordCountJob.WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //通过job设置combiner处理类, 与reduce相仿
        job.setCombinerClass(WordCountJob.WordCountReducer.class);

        FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));

        boolean flag = job.waitForCompletion(true);
        System.out.println("flag: " + flag);
        System.exit(flag ? 0 : 1);
    }
}
