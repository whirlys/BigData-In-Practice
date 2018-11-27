// cc FileCopyWithProgress Copies a local file to a Hadoop filesystem, and shows progress

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * 工具类 IOUtils 上传文件到 HDFS，并打印进度条
 */
public class FileCopyWithProgress {
    public static void main(String[] args) throws Exception {
        String localSrc = "hello.txt";
        String dst = "/hadoop/hello.txt";

        InputStream in = new BufferedInputStream(new FileInputStream(localSrc));

        FileSystem fs = SysUtil.getFileSystem();
        OutputStream out = fs.create(new Path(dst), new Progressable() {
            @Override
            public void progress() {
                System.out.print(".");
            }
        });

        IOUtils.copyBytes(in, out, 4096, true);
    }
}