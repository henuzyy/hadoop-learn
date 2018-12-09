package com.zyy.hadoop.mapreduce.wc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Administrator on 2018/10/1.
 */
public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<LongWritable> iterator = values.iterator();
        long count = 0;

        while (iterator.hasNext()) {
            //注意这里需要遍历出来，否则进程会卡住
            LongWritable longWritable = iterator.next();
            count += longWritable.get();
        }
        context.write(key, new LongWritable(count));
    }
}
