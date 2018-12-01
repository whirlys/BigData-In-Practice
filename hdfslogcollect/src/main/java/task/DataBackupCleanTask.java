package task;

import config.GlobalConstants;
import org.apache.commons.io.FileUtils;
import util.PropsHolder;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimerTask;

/**
 * @program: hdfslogcollect
 * @description: 备份日志清理任务:
 * 遍历日志备份目录，获取其中的子文件夹；
 * 通过各子文件夹的名字转换成备份文件夹的日期；
 * 判断备份文件夹的备份时间距当前时间的时间差是否超出备份最长时限，如果超过最大允许时限，则删除该备份子目录；
 * @author: 赖键锋
 * @create: 2018-12-01 18:08
 **/
public class DataBackupCleanTask extends TimerTask {

    @Override
    public void run() {
        Date date = new Date();
        System.out.println("备份处理线程启动，当前时间： " + date);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");

        try {
            Properties prop = PropsHolder.getProperties();
            // 获取一级备份目录
            File backupBaseDir = new File(prop.getProperty(GlobalConstants.LOG_BAK_BASE_PATH));
            // 获取一级备份目录下的所有二级子目录
            File[] backupDirs = backupBaseDir.listFiles(); // 2017-08-15-10
            // 遍历所有二级子目录
            for (File dir : backupDirs) {
                Date backDate = sdf.parse(dir.getName());
                // 计算子备份目录的备份时间是否已超24小时
                if (date.getTime() - backDate.getTime() > 24 * 60 * 60 * 1000L) {
                    // 如果超出24小时，则删除
                    System.out.println("探测到需要清除的备份文件夹：" + dir.getPath());
                    FileUtils.deleteQuietly(dir);
                    System.out.println("成功删除");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
