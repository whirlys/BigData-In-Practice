package job2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: commonfans
 * @description:
 * @author: 赖键锋
 * @create: 2018-12-01 21:28
 **/
public class CommonFansStep2Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text pair, Iterable<Text> fans, Context context) throws IOException, InterruptedException {
        // 构造一个StringBuilder用于拼接字符串
        StringBuilder sb = new StringBuilder();

        // 将这个用户对的所有共同好友拼接在一起
        for (Text f : fans) {
            sb.append(f).append(",");
        }

        // 将用户对作为key，拼接好的所有共同粉丝作为value，返回给reduce task
        context.write(pair, new Text(sb.substring(0, sb.length() - 1)));
    }
}
