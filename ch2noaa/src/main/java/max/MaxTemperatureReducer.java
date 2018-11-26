package max;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: ch2noaa
 * @description:
 * @author: 赖键锋
 * @create: 2018-11-26 12:42
 **/
public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxValue = Integer.MIN_VALUE;
        for (IntWritable value : values) {
            // 最大值
            maxValue = Math.max(maxValue, value.get());
        }
        context.write(key, new IntWritable(maxValue));

    }
}
