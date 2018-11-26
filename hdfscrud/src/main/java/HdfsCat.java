import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @program: hdfscrud
 * @description: 使用字符流读取数据
 * @author: 赖键锋
 * @create: 2018-11-26 23:14
 **/
public class HdfsCat {

    /**
     * 查看 HDFS 文件内容
     */
    public static void cat(String remotePathStr) throws IOException {
        final FileSystem fileSystem = SysUtil.getFileSystem();
        Path remotePath = new Path(remotePathStr);
        FSDataInputStream inputStream = fileSystem.open(remotePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        String line = null;
        StringBuffer buffer = new StringBuffer();
        while ((line = reader.readLine()) != null) {
            buffer.append(line + "\n");
        }
        System.out.println(buffer.toString());
        fileSystem.close();
    }

    public static void main(String[] args) throws IOException {
        String remotePathStr = "/hadoop/upload.txt";
        System.out.println(String.format("查看HDFS文件《%s》的内容", remotePathStr));
        cat(remotePathStr);
        System.out.println("完成！");
    }
}
