package com.zyy.hadoop.mapreduce.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;


/**
 * Created by Administrator on 2018/10/1.
 */
//public class WCRunner {
public class WCRunner extends Configured implements Tool {

    private Class<Text> theClass;

    public static void main(String[] args) throws Exception {
        System.exit(new WCRunner().run(args));

//        Configuration configuration = new Configuration();
//
//        Job wcJob = Job.getInstance(configuration);
//
//        wcJob.setJarByClass(WCRunner.class);
//
//        wcJob.setMapperClass(WCMapper.class);
//        wcJob.setReducerClass(WCReducer.class);
//
//        wcJob.setMapOutputKeyClass(Text.class);
//        wcJob.setMapOutputValueClass(LongWritable.class);
//
//        //如果Map 和reduce 一样的类型，可以直接使用这个
//        wcJob.setOutputKeyClass(Text.class);
//        wcJob.setOutputValueClass(LongWritable.class);
//
//        FileInputFormat.setInputPaths(wcJob, new Path(args[0]));
//        FileOutputFormat.setOutputPath(wcJob, new Path(args[1]));
//
//        System.exit(wcJob.waitForCompletion(true) ? 0 : 1);

    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = new Configuration();

        Job wcJob = Job.getInstance(configuration);

        wcJob.setJarByClass(WCRunner.class);

        wcJob.setMapperClass(WCMapper.class);
        wcJob.setReducerClass(WCReducer.class);

        wcJob.setMapOutputKeyClass(theClass);
        wcJob.setMapOutputValueClass(LongWritable.class);

        //如果Map 和reduce 一样的类型，可以直接使用这个
        wcJob.setOutputKeyClass(Text.class);
        wcJob.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(wcJob, new Path(strings[0]));
        FileOutputFormat.setOutputPath(wcJob, new Path(strings[1]));

        return wcJob.waitForCompletion(true) ? 0 : 1;
    }
}
