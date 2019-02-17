package com.whirly.hbase.api;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 * @program: HbaseExamples
 * @description:
 * @author: 赖键锋
 * @create: 2019-02-16 14:04
 **/
public class HBaseUtilTest {
    @Test
    public void createTable() {
        HBaseUtil.createTable("FileTable", new String[]{"fileInfo", "saveInfo"});
    }

    @Test
    public void addFileDetails() {
        HBaseUtil.putRow("FileTable", "rowkey1", "fileInfo", "name", "file1.txt");
        HBaseUtil.putRow("FileTable", "rowkey1", "fileInfo", "type", "txt");
        HBaseUtil.putRow("FileTable", "rowkey1", "fileInfo", "size", "1024");
        HBaseUtil.putRow("FileTable", "rowkey1", "saveInfo", "creator", "whirly");
        HBaseUtil.putRow("FileTable", "rowkey2", "fileInfo", "name", "file2.jpg");
        HBaseUtil.putRow("FileTable", "rowkey2", "fileInfo", "type", "jpg");
        HBaseUtil.putRow("FileTable", "rowkey2", "fileInfo", "size", "1024");
        HBaseUtil.putRow("FileTable", "rowkey2", "saveInfo", "creator", "whirly");

    }

    @Test
    public void getFileDetails() {
        Result result = HBaseUtil.getRow("FileTable", "rowkey1");
        if (result != null) {
            System.out.println("rowkey=" + Bytes.toString(result.getRow()));
            System.out.println("fileName=" + Bytes
                    .toString(result.getValue(Bytes.toBytes("fileInfo"), Bytes.toBytes("name"))));
        }
    }

    @Test
    public void scanFileDetails() {
        ResultScanner scanner = HBaseUtil.getScanner("FileTable", "rowkey2", "rowkey2");
        if (scanner != null) {
            scanner.forEach(result -> {
                System.out.println("rowkey=" + Bytes.toString(result.getRow()));
                System.out.println("fileName=" + Bytes
                        .toString(result.getValue(Bytes.toBytes("fileInfo"), Bytes.toBytes("name"))));
            });
            scanner.close();
        }
    }

    @Test
    public void deleteRow() {
        HBaseUtil.deleteRow("FileTable", "rowkey1");
    }

    @Test
    public void deleteTable() {
        HBaseUtil.deleteTable("FileTable");
    }
}
