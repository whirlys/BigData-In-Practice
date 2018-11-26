import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * @program: hdfscrud
 * @description:
 * @author: 赖键锋
 * @create: 2018-11-26 20:57
 **/
public class HdfsUpload {
    private static final FileSystem fileSystem = SysUtil.getFileSystem();

    /**
     * 判断文件是否已存在
     */
    public static boolean exist(String file) {
        Path remotePath = new Path(file);
        try {
            return fileSystem.exists(remotePath);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 复制文件，从本地复制到HDFS中
     */
    public static void uploadToHdfs(String localPathStr, String hdfsPathStr) {
        Path localPath = new Path(localPathStr);
        Path remotePath = new Path(hdfsPathStr);

        // 第一个参数表示是否删除源文件，第二个参数表示是否覆盖
        try {
            fileSystem.copyFromLocalFile(false, true, localPath, remotePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 追加文件，从本地追加到 HDFS 的文件中
     */
    public static void appendToFile(String localPathStr, String hdfsPathStr) {
        Path hdfsPath = new Path(hdfsPathStr);
        try (FileInputStream inputStream = new FileInputStream(localPathStr)) {
            FSDataOutputStream outputStream = fileSystem.append(hdfsPath);
            byte[] data = new byte[1024];
            int read = -1;
            while ((read = inputStream.read(data)) > 0) {
                outputStream.write(data, 0, read);
            }
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 查看 HDFS 文件中的内容
     */
    public static void catHdfsFile(String remotePathStr) {
        Path path = new Path(remotePathStr);
        try (FSDataInputStream inputStream = fileSystem.open(path)) {
            byte[] data = new byte[2014];
            int read = -1;
            while ((read = inputStream.read(data)) > 0) {
                System.out.println(new String(data, 0 ,read));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        String localPathStr = "./hello.txt";
        String remotePathStr = "/hadoop/upload.txt";

        boolean override = false;
        boolean isExist = exist(remotePathStr);

        if (!isExist) {
            // 文件不存在，上传文件
            uploadToHdfs(localPathStr, remotePathStr);
            System.out.println(String.format("文件《%s》上传成功！上传路径《%s》", localPathStr, remotePathStr));
            // 查看文件内容： hadoop dfs -cat /hadoop/upload.txt
            catHdfsFile(remotePathStr);
        } else if (override == true) {
            // 覆盖文件
            System.out.println(String.format("文件《%s》已存在！将覆盖！", remotePathStr));
            uploadToHdfs(localPathStr, remotePathStr);
            System.out.println("文件上传完成！");
            catHdfsFile(remotePathStr);
        } else {
            // 追加
            System.out.println(String.format("文件《%s》已存在！将追加！", remotePathStr));
            appendToFile(localPathStr, remotePathStr);
            System.out.println("文件追加完成！");
            catHdfsFile(remotePathStr);
        }

        fileSystem.close();
    }
}
