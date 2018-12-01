package job1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * @program: commonfans
 * @description:
 * @author: 赖键锋
 * @create: 2018-12-01 21:03
 **/
public class CommonFansStep1Reducer extends Reducer<Text, Text, Text, Text> {
    // fan:某个"粉丝"
    // users: fan关注的一组用户字符串： user1,user2,user3
    @Override
    protected void reduce(Text fan, Iterable<Text> users, Context context) throws IOException, InterruptedException {
        ArrayList<Text> userList = new ArrayList<>();

        // 将粉丝关注的一组用户从迭代器中取出，放入一个arraylist暂存
        for (Text user : users) {
            userList.add(new Text(user));
        }

        // 对users排个序，以免拼俩俩对时出现A-F 又有F-A的现象
        Collections.sort(userList);

        // 把这一对user进行两两组合，并将:
        //1.组合作为key
        //2.共同的粉丝fan作为value
        //返回给reduce task作为本job的最终结果
        for (int i = 0; i < userList.size(); i++) {
            // 前面排过序，这里 j 从 i + 1 开始，所以不会出现 有 A-F 又有 F-A 的现象
            for (int j = i + 1; j < userList.size(); j++) {
                // 输出 "用户-用户" 两两对，及他俩的共同粉丝
                context.write(new Text(userList.get(i) + "-" + userList.get(j)), fan);
            }

        }
    }
}
