package com.whirly.hbase.api;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;

import java.io.IOException;

/**
 * @program: HbaseExamples
 * @description:
 * @author: 赖键锋
 * @create: 2019-02-16 13:58
 **/
public class HBaseConnTest {
    @Test
    public void getConnTest() {
        Connection conn = HBaseConn.getHBaseConn();
        System.out.println(conn.isClosed());
        HBaseConn.closeConn();
        System.out.println(conn.isClosed());
    }

    @Test
    public void getTableTest() {
        try {
            Table table = HBaseConn.getTable("US_POPULATION");
            System.out.println(table.getName().getNameAsString());
            table.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
}
