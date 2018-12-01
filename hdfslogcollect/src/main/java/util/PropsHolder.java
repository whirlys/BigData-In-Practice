package util;

import java.io.IOException;
import java.util.Properties;

/**
 * @program: hdfslogcollect
 * @description:
 * @author: 赖键锋
 * @create: 2018-12-01 11:20
 **/
public class PropsHolder {
    private static Properties properties = null;

    /**
     * 单例模式，双重检查锁 获取 Properties
     */
    public static Properties getProperties() throws IOException {
        if (properties == null) {
            synchronized (PropsHolder.class) {
                if (properties == null) {
                    properties = new Properties();
                    properties.load(PropsHolder.class.getClassLoader().getResourceAsStream("datacollect.properties"));
                }
            }
        }
        return properties;
    }
}
