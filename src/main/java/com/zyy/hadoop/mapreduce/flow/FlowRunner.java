package com.zyy.hadoop.mapreduce.flow;

import com.zyy.hadoop.mapreduce.flow.bean.FlowBean;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Created by Administrator on 2018/10/2.
 */
public class FlowRunner extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(FlowRunner.class);

    public static void main(String[] args) throws Exception {
        int result = new FlowRunner().run(args);
        LOG.info("计算任务结束，状态码:" + result);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job flowRunner = Job.getInstance(configuration);

        flowRunner.setJarByClass(FlowRunner.class);

        flowRunner.setMapperClass(FlowMapper.class);
        flowRunner.setReducerClass(FlowReducer.class);

        flowRunner.setMapOutputKeyClass(LongWritable.class);
        flowRunner.setMapOutputValueClass(FlowBean.class);

        flowRunner.setOutputKeyClass(Text.class);
        flowRunner.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(flowRunner, new Path(args[0]));
        FileOutputFormat.setOutputPath(flowRunner, new Path(args[1]));

        return flowRunner.waitForCompletion(true) ? 0 : -1;
    }


    public static class FlowReducer extends Reducer<LongWritable, FlowBean, Text, NullWritable> {

        @Override
        protected void reduce(LongWritable key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long upStream = 0;
            long downStream = 0;
            for (FlowBean flowBean : values) {
                upStream += flowBean.getUpStream();
                downStream += flowBean.getDownStream();
            }

            String result = key + " " + upStream + " " + downStream;
            LOG.info("result:" + result);
            context.write(new Text(result), NullWritable.get());
        }
    }


    public class FlowMapper extends Mapper<LongWritable, Text, LongWritable, FlowBean> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
//        String[] fields = line.split(" ");
//        String[] fields= StringUtils.split(line, "\t");

            String[] fields = StringUtils.split(line, "\t");
            LOG.info("line:" + key.get() + " size:" + fields.length  + " value:" + line);
            long phone = Long.parseLong(fields[1].trim());
            long upStream = Long.parseLong(fields[7].trim());
            long downStream = Long.parseLong(fields[8].trim());
            String name = phone + "," + key.get();
            LOG.info("line:" + key + " name=" + name);
            context.write(new LongWritable(phone), new FlowBean(phone, upStream, downStream, name));

        }
    }

}
