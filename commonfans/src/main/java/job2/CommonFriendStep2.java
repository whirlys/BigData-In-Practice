package job2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @program: commonfans
 * @description:
 * @author: 赖键锋
 * @create: 2018-12-01 21:31
 **/
public class CommonFriendStep2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(CommonFriendStep2.class);
        // 设置job的mapper类和reducer类
        job.setMapperClass(CommonFansStep2Mapper.class);
        job.setReducerClass(CommonFansStep2Reducer.class);

        // 设置map阶段输出key:value数据的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置reudce阶段输出key:value数据的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 检测输出目录是否已存在，如果已存在则删除，以免在测试阶段需要反复手动删除输出目录
        FileSystem fs = FileSystem.get(conf);
        Path out = new Path(args[1]);
        if(fs.exists(out)) {
            fs.delete(out, true);
        }

        // 设置数据输入输出目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,out);

        // 提交job到yarn或者local runner执行
        job.waitForCompletion(true);

    }
}
