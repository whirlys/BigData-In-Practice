import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;

/**
 * @program: hdfscrud
 * @description:
 * @author: 赖键锋
 * @create: 2018-11-26 20:54
 **/
public class SysUtil {
    public static FileSystem getFileSystem() {
        try {
            // 设置HDFS的URL和端口号
            String HDFSURL = "hdfs://ip:8020";
            // HDFS的配置信息
            Configuration configuration = new Configuration();
            // 设置副本数为 1
            configuration.set("dfs.replication", "1");
            // //获取文件系统
            FileSystem fileSystem = FileSystem.get(URI.create(HDFSURL), configuration);

            return fileSystem;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
