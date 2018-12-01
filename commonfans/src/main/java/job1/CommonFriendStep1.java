package job1;

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
 * @create: 2018-12-01 21:17
 **/
public class CommonFriendStep1 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(CommonFriendStep1.class);
        // 设置job的mapper类
        job.setMapperClass(CommonFansStep1Mapper.class);
        // 设置job的reducer类
        job.setReducerClass(CommonFansStep1Reducer.class);

        // 设置map阶段输出的key：value数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置reduce阶段输出的key：value数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 判断结果输出路径是否已存在，如果已经存在，则删除。以免在测试阶段需要反复手动删除输出目录
        FileSystem fs = FileSystem.get(configuration);
        Path out = new Path(args[1]);
        if (fs.exists(out)) {
            fs.delete(out, true);
        }

        // 设置数据输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, out);

        // 提交job给yarn或者local runner来运行
        job.waitForCompletion(true);
    }
}
