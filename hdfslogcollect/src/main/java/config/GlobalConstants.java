package config;

/**
 * @program: hdfslogcollect
 * @description:
 * @author: 赖键锋
 * @create: 2018-12-01 11:19
 **/
public class GlobalConstants {
    // 日志源路径
    public static final String LOG_SRC_PATH = "LOG_SRC_PATH";
    // 日志备份路径
    public static final String LOG_BAK_BASE_PATH = "LOG_BAK_BASE_PATH";
    // 日志文件名前缀
    public static final String LOG_SRC_FILE_PREFIX = "LOG_SRC_FILE_PREFIX";
    // 日志文件待上传临时存储路径
    public static final String LOG_TOUPLOAD = "LOG_TOUPLOAD";
    // 目标HDFS集群URI
    public static final String HDFS_URI = "HDFS_URI";
    // 日志文件目标存储路径
    public static final String LOG_HDFS_BASE_PATH = "LOG_HDFS_BASE_PATH";
    // 日志文件名目标存储名前缀
    public static final String LOG_HDFS_FILE_PREFIX = "LOG_HDFS_FILE_PREFIX";
    // 日志文件名目标存储名后缀
    public static final String LOG_HDFS_FILE_SUFFIX = "LOG_HDFS_FILE_SUFFIX";
}
