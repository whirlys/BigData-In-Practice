## HDFS简单案例 - 日志数据采集

> 来源：[实战案例玩转Hadoop系列7--HDFS实战应用案例：数据采集](https://zhuanlan.zhihu.com/p/49791428)

#### 需求
需要一个日志文件采集系统，定期地将日志文件从各台web服务器的本地磁盘目录中传输到HDFS集群上去



上传的关键代码 copyFromLocalFile



```jva
// 遍历待上传临时目录中的每一个文件
for (File file : toUploadFiles) {
	// 生成目标文件名
	String dstName = prop.getProperty(GlobalConstants.LOG_HDFS_FILE_PREFIX)+UUID.randomUUID()+prop.getProperty(GlobalConstants.LOG_HDFS_FILE_SUFFIX);

	// 拼接hdfs上的完整的目标存储路径
	Path destPath = new Path(prop.getProperty(GlobalConstants.LOG_HDFS_BASE_PATH)+dayString+"/"+dstName);

	// 上传该文件
	System.out.println("准备上传：" + file.getPath());
	fs.copyFromLocalFile(new Path(file.getPath()), destPath);
	System.out.println("上传完毕");

	// 生成备份目录路径： 备份文件按小时分文件夹存放
	File backupDir = new File(prop.getProperty(GlobalConstants.LOG_BAK_BASE_PATH)+dayHourString);
	System.out.println("即将备份到：" + backupDir.getPath());

	// 移动到备份目录
	FileUtils.moveFileToDirectory(file, backupDir, true);
	System.out.println("备份完毕：" + backupDir.getPath());
}
```

