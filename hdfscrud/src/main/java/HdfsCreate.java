import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * @program: hdfscrud
 * @description: HDFS创建文件
 * @author: 赖键锋
 * @create: 2018-11-26 20:36
 **/
public class HdfsCreate {
    public static void main(String[] args) throws IOException {
        //获取文件系统
        FileSystem fileSystem = SysUtil.getFileSystem();

        // 如果因为权限而无法写入，可以先修改权限 hadoop dfs -chmod 777 /hadoop
        Path path = new Path("/hadoop/create.txt");
        // 获取输出流
        FSDataOutputStream outputStream = fileSystem.create(path);
        // 写入一些内容
        outputStream.writeUTF("Hello HDFS！");
        outputStream.close();

        // ------写入完毕后，再读出来-----------
        // 获取该文件的输入流
        FSDataInputStream inputStream = fileSystem.open(path);
        String data = inputStream.readUTF();
        System.out.println(data);
        // 输出： Hello HDFS！

        fileSystem.close();
    }
}
