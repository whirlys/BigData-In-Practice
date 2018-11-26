import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @program: hdfscrud
 * @description: 从 HDFS 上下载文件到本地
 * @author: 赖键锋
 * @create: 2018-11-26 22:29
 **/
public class HdfsDownload {
    private static final FileSystem fileSystem = SysUtil.getFileSystem();

    /**
     * 从 HDFS 下载文件到本地
     */
    public static void download(String remotePathStr, String prefix, String suffix) throws IOException {
        String localFileStr = prefix + "." + suffix;
        File localFile = new File(localFileStr);
        while (localFile.exists()) {
            // 如果本地文件已存在，需要重命名
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmssS");
            String tempLocalFileStr = prefix + "-" + dateFormat.format(new Date()) + "." + suffix;
            System.out.println(String.format("文件《%s》已存在，将重命名为《%s》", localFile.getName(), tempLocalFileStr));
            localFile = new File(tempLocalFileStr);
        }

        System.out.println(localFile.getAbsolutePath());
        System.out.println(localFile.getName());
        Path remotePath = new Path(remotePathStr);
        Path localPath = new Path(localFile.getName());

        // 复制到本地，第一个参数：是否删除原文件，第四个参数：是否使用 RawLocalFileSystem
        fileSystem.copyToLocalFile(false, remotePath, localPath, true);
    }

    public static void main(String[] args) throws IOException {
        String remotePathStr = "/hadoop/upload.txt";

        download(remotePathStr, "copyFile", "txt");

        System.out.println("下载完成！");
        fileSystem.close();
    }
}
