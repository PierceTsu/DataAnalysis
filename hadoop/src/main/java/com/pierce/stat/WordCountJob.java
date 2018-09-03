package com.pierce.stat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @Project : wordcount
 * @Package Name : com.pierce.stat
 * @Description: WordCountJob
 * @Author : piercetsu@gmail.com
 * @Create Date: 2018-09-03
 */
public class WordCountJob {

    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //方式一
//            StringTokenizer itr = new StringTokenizer(value.toString());
//            while (itr.hasMoreTokens()) {
//                word.set(itr.nextToken());
//                context.write(word, one);
//            }

            //方式二
            String line = value.toString();
            String[] words = line.split(" ");
            for (String word : words) {
                context.write(new Text(word), one);
            }

        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
