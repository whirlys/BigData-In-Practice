import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @program: customizePartition
 * @description:
 * @author: 赖键锋
 * @create: 2018-12-02 16:58
 **/
public class JiduRunner {

    /**
     * mapper，简单的将一行数据切分为 <部门：绩效> 键值对
     */
    static class JiduMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] splits = line.split("\t");

            context.write(new Text(splits[0]), new IntWritable(Integer.parseInt(splits[1])));
        }
    }

    /**
     * 自定义分区类将相同部门的数据分发到相同的 reducer 里边，现在可以汇总部门的全年绩效了
     */
    static class JiduReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                // 汇总部门全年的绩效
                sum = sum + v.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(JiduRunner.class);
        job.setMapperClass(JiduMapper.class);
        job.setReducerClass(JiduReducer.class);
        job.setCombinerClass(JiduReducer.class);

        // 设置自定义分区类，不设置默认为 HashPartitioner
        job.setPartitionerClass(JiduPartitioner.class);

        //设置reduce task数量为4 + 1
        job.setNumReduceTasks(5);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        Path out = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(out)) {
            fs.delete(out, true);
        }
        FileOutputFormat.setOutputPath(job, out);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
