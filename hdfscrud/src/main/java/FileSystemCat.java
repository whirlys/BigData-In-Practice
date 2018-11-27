// cc FileSystemCat Displays files from a Hadoop filesystem on standard output by using the FileSystem directly

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;

/**
 * IOUtils 复制 HDFS 文件到 控制台，（也可以复制到本地）
 */
public class FileSystemCat {

  public static void main(String[] args) throws Exception {
    String uri = "/hadoop/hello.txt";
    FileSystem fs = SysUtil.getFileSystem();
    InputStream in = null;
    try {
      in = fs.open(new Path(uri));
      IOUtils.copyBytes(in, System.out, 4096, false);
    } finally {
      IOUtils.closeStream(in);
    }
  }
}
