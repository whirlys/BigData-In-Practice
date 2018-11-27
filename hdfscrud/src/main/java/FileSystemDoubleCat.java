// cc FileSystemDoubleCat Displays files from a Hadoop filesystem on standard output twice, by using seek

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * 复制 HDFS 文件到控制台，并通过 seek() 方法用于移动文件读取指针到文件头，重复复制
 */
public class FileSystemDoubleCat {

    public static void main(String[] args) throws Exception {
        String uri = "/hadoop/hello.txt";
        FileSystem fs = SysUtil.getFileSystem();
        FSDataInputStream in = null;
        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
            System.out.println("\n\n-----------seek到文件头，再一次复制----------------\n\n");
            // go back to the start of the file
            in.seek(0);
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
