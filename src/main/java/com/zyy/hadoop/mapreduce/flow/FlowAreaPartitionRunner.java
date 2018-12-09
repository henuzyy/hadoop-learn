package com.zyy.hadoop.mapreduce.flow;

import com.zyy.hadoop.mapreduce.flow.bean.FlowBean;
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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2018/10/2.
 */
public class FlowAreaPartitionRunner extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(FlowAreaPartitionRunner.class);

    public static void main(String[] args) {
        int statu = 0;
        try {
            statu = new FlowAreaPartitionRunner().run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.info("计算结束，状态码：" + statu);
        System.exit(statu);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);


        job.setJarByClass(FlowAreaPartitionRunner.class);

        job.setPartitionerClass(AreaPartition.class);
        /**
         * reduce的task可以和分组的个数一致，或者大于分组的个数，但是不能小于分组的个数，因为根据分组后程序找不到对应的reducer进程；
         * 但是设置1是一个特例，因为只有这一个分组，程序只能放入到这里
         * */
        job.setNumReduceTasks(6);

        job.setMapperClass(AreaMapper.class);
        job.setReducerClass(AreaReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        return job.waitForCompletion(true) ? 0 : -1;
    }


    private static class AreaMapper extends Mapper<LongWritable, Text, LongWritable, FlowBean> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = StringUtils.split(line, ' ');
            long phone = Long.parseLong(fields[0]);
            long upStrem = Long.parseLong(fields[1]);
            long dowStrem = Long.parseLong(fields[2]);

            String name = "line:" + key.get() + " value:" + line;
            LOG.info(name);
            context.write(new LongWritable(phone), new FlowBean(phone, upStrem, dowStrem, name));
        }
    }

    private static class AreaReducer extends Reducer<LongWritable, FlowBean, Text, NullWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long upSteams = 0;
            long downStreams = 0;
            for (FlowBean flowBean : values) {
                upSteams += flowBean.getUpStream();
                downStreams += flowBean.getDownStream();
            }

            String result = "reduce:  key:" + key.get() + " result:  upStreams=" + upSteams + ", downStreams=" + downStreams;
            LOG.info(result);

            context.write(new Text(key.get() + " " + upSteams + " " + downStreams), NullWritable.get());
        }
    }

    public static class AreaPartition<KEY, VALUE> extends Partitioner<KEY, VALUE> {
        private static final Map<String, Short> SECTION = new HashMap<>();

        static {
            SECTION.put("133", (short) 1);
            SECTION.put("134", (short) 2);
            SECTION.put("135", (short) 3);
            SECTION.put("136", (short) 4);
            SECTION.put("138", (short) 0);
        }

        @Override
        public int getPartition(KEY key, VALUE value, int numPartitions) {

            String suffix = key.toString().substring(0, 3);

            return getSlot(suffix);
        }


        public static int getSlot(String suffix) {
            Short slot = SECTION.get(suffix);
            if (slot == null) {
                return 5;
            }
            return slot;

        }
    }

}
