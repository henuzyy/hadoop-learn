package com.zyy.hadoop.mapreduce.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Administrator on 2018/10/4.
 */
public class WCLocalRunner {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        /**
         *
         * 本地运行，一定不要再类路径下放hdfs相关的配置文件，否则就会被解析为hdfs格式
         *
         * **/
        Configuration configuration = new Configuration();

        Job wcJob = Job.getInstance(configuration);

        wcJob.setJarByClass(WCRunner.class);

        wcJob.setMapperClass(WCMapper.class);
        wcJob.setReducerClass(WCReducer.class);

        wcJob.setMapOutputKeyClass(Text.class);
        wcJob.setMapOutputValueClass(LongWritable.class);

        //如果Map 和reduce 一样的类型，可以直接使用这个
        wcJob.setOutputKeyClass(Text.class);
        wcJob.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(wcJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(wcJob, new Path(args[1]));


        System.exit(wcJob.waitForCompletion(true) ? 0 : 1);
    }
}
