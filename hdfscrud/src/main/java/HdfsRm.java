import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;

/**
 * @program: hdfscrud
 * @description: 删除HDFS文件
 * @author: 赖键锋
 * @create: 2018-11-26 23:42
 **/
public class HdfsRm {

    /**
     * 永久性删除指定的文件或目录，如果remotePathStr是一个空目录或者文件，那么recursive的值就会被忽略。
     * 只有recursive＝true的时候，一个非空目录及其内容才会被删除（即递归删除所有文件）
     *
     * @param remotePathStr 是否递归删除
     * @param recursive
     */
    public static void delete(String remotePathStr, boolean recursive) throws IOException {
        Path remotePath = new Path(remotePathStr);
        FileSystem fileSystem = SysUtil.getFileSystem();
        boolean result = fileSystem.delete(remotePath, recursive);
        if (result) {
            System.out.println("删除成功！");
        } else {
            System.out.println("删除失败！");
        }
        fileSystem.close();
    }

    /**
     * 列出文件夹下的所有文件
     */
    public static void listFiles(String remotePathStr, boolean recursive) throws IOException {
        Path remotePath = new Path(remotePathStr);
        FileSystem fileSystem = SysUtil.getFileSystem();

        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(remotePath, recursive);
        System.out.println(String.format("文件夹《%s》下的所有文件：", remotePathStr));
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
        fileSystem.close();
    }

    public static void main(String[] args) throws IOException {
        // String remotePathStr = "/hadoop/create.txt";
        String remotePathStr = "/hadoop/ch2";

        listFiles(remotePathStr, true);
        // delete(remotePathStr, true);
    }

}
