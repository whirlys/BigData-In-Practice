package com.whirly.kafka.ch10_monitor;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;

/**
 * @description: 需要开启 JMX
 * @author: 赖键锋
 * @create: 2019-04-19 15:38
 **/
public class JmxConnectionDemo {
    private MBeanServerConnection conn;
    private String jmxURL;
    private String ipAndPort;

    public JmxConnectionDemo(String ipAndPort) {
        this.ipAndPort = ipAndPort;
    }

    public boolean init(){
        jmxURL = "service:jmx:rmi:///jndi/rmi://" + ipAndPort + "/jmxrmi";
        try {
            JMXServiceURL serviceURL = new JMXServiceURL(jmxURL);
            JMXConnector connector = JMXConnectorFactory
                    .connect(serviceURL, null);
            conn = connector.getMBeanServerConnection();
            if (conn == null) {
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }



    public double getMsgInPerSec() {
        String objectName = "kafka.server:type=BrokerTopicMetrics," +
                "name=MessagesInPerSec";
        Object val = getAttribute(objectName, "OneMinuteRate");
        if (val != null) {
            return (double) (Double) val;
        }
        return 0.0;
    }

    private Object getAttribute(String objName, String objAttr) {
        ObjectName objectName;
        try {
            objectName = new ObjectName(objName);
            return conn.getAttribute(objectName, objAttr);
        } catch (MalformedObjectNameException | IOException |
                ReflectionException | InstanceNotFoundException |
                AttributeNotFoundException | MBeanException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        JmxConnectionDemo jmxConnectionDemo =
                new JmxConnectionDemo("192.168.0.101:9999");
        jmxConnectionDemo.init();
        System.out.println(jmxConnectionDemo.getMsgInPerSec());
    }
}
