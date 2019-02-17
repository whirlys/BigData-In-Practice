package com.whirly.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @program: hbaseintro
 * @description: HBase Java API 增删查改
 * 参考 https://blog.csdn.net/sinat_39409672/article/details/78403015
 * @author: 赖键锋
 * @create: 2018-11-29 21:40
 **/
public class FirstClient {
    public static Connection connection;

    // 用 HBaseconfiguration 初始化配置信息是会自动加载当前应用的 classpath 下的 hbase-site.xml
    public static Configuration configuration;

    /**
     * 对connection进行初始化、当然也可以手动加载配置文件，手动加载配置文件时要调用configuration的addResource方法
     */
    public static void init() throws IOException {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master:2181");
        // configuration.addResource("hbase-site.xml");
        connection = ConnectionFactory.createConnection(configuration);
    }

    /**
     * Java API 创建表
     */
    public static void createTable(String tableName, String... cfs) throws IOException {
        Admin admin = connection.getAdmin();
        // HTD需要 TableName 类型的 tableName，创建 TableName 类型的 tableName
        TableName tbName = TableName.valueOf(tableName);

        // 判断表是否已经存在，不存在则创建表
        if (admin.tableExists(tbName)) {
            System.err.println(String.format("表 %s 已经存在！", tableName));
            return;
        }
        // 通过 HTableDescriptor 创建一个表的描述
        HTableDescriptor HTDescriptor = new HTableDescriptor(tbName);
        // 为描述器添加表的详细参数
        for (String cf : cfs) {
            // 创建 HColumnDescriptor 对象添加表的详细的描述
            HColumnDescriptor hcd = new HColumnDescriptor(cf);
            HTDescriptor.addFamily(hcd);
        }
        // 调用createtable方法创建表
        admin.createTable(HTDescriptor);
        System.out.println(String.format("表 %s 创建成功！", tableName));
    }

    /**
     * Java API 删除表
     */
    public static void deleteTable(String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        TableName tbName = TableName.valueOf(tableName);

        // 判断表是否已经存在，不存在则退出
        if (!admin.tableExists(tbName)) {
            System.err.println(String.format("表 %s 不存在！", tableName));
            return;
        }
        // 首先将表解除占用，否则无法删除
        admin.disableTable(tbName);
        // 删除
        admin.deleteTable(tbName);
        System.out.println(String.format("表 %s 已经删除！", tableName));
    }

    /**
     * Java API 向表写入数据
     */
    public static void putData(String tableName, String rowkey, String family, String qualifier, String value) throws IOException {
        TableName tbName = TableName.valueOf(tableName);
        // 通过connection获取相应的表
        Table table = connection.getTable(tbName);

        // 实例化put对象，传入行键
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
        // 调用put方法将一行数据写入hbase，更加有效率的是批量写 table.put(puts)
        table.put(put);
        System.out.println("数据插入完成！插入内容：" + put.toString());
    }

    /**
     * Java API 查询数据
     */
    public static void getData(String tableName, String rowkey) throws IOException {
        TableName tbName = TableName.valueOf(tableName);
        Table table = connection.getTable(tbName);

        // 创建Get, 查询的条件
        Get get = new Get(Bytes.toBytes(rowkey));

        // 查询，更有效率的是批量查询 table.get( List<Get> )
        Result result = table.get(get);

        // 调用result.cellscanner创建scanner对象
        CellScanner cellScanner = result.cellScanner();

        // 遍历结果集，取出查询结果，
        // 如果存在下一个cell则advandce方法返回true，且current方法会返回一个有效的cell，可以当作循环条件
        while (cellScanner.advance()) {
            // current 方法返回一个有效的 cell
            Cell cell = cellScanner.current();
            // 使用CellUtil调用相应的方法获取想用的数据，并利用Bytes.toString方法将结果转换为String输出
            String row = Bytes.toString(CellUtil.cloneRow(cell));
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String qualify = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println(String.format("[%s, %s, %s, %s]", row, family, qualify, value));
        }
    }

    /**
     * Java API 删除某条数据
     */
    public static void deleteData(String tableName, String rowkey, String family, String qualifier) throws IOException {
        TableName tbName = TableName.valueOf(tableName);
        Table table = connection.getTable(tbName);

        // 创建delete对象
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        // 将要删除的数据的准确坐标添加到对象中
        delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        //删除
        table.delete(delete);
        System.out.println(String.format("删除数据 [%s, %s, %s, %s]", tableName, rowkey, family, qualifier));
    }

    /**
     * 删除一行数据
     */
    public static void deleteRow(String tableName, String rowkey) throws IOException {
        TableName tbName = TableName.valueOf(tableName);
        Table table = connection.getTable(tbName);

        // 通过行键删除一整行的数据
        Delete deleteRow = new Delete(Bytes.toBytes(rowkey));
        table.delete(deleteRow);
    }

    public static void main(String[] args) {
        String tableName = "member";
        try {
            // 初始化
            init();
            System.out.println("\n--------建表-------\n");
            // 建表，表名 member，列族有两列 info 和 address
            createTable(tableName, "info", "address");

            System.out.println("\n--------写入数据-------\n");
            // 写入数据
            putData(tableName, "001", "info", "name", "小旋锋");
            putData(tableName, "001", "info", "age", "1");
            putData(tableName, "001", "info", "sex", "man");
            putData(tableName, "001", "address", "city", "dongguan");

            putData(tableName, "002", "info", "name", "赖键锋");
            putData(tableName, "002", "info", "WeChat-Subscription", "小旋锋");
            putData(tableName, "002", "address", "city", "深圳");

            System.out.println("\n-------- 查看数据001-------\n");
            // 查看数据
            getData(tableName, "001");

            System.out.println("\n-------删除数据 001 info age--------\n");
            // 删除数据
            deleteData(tableName, "001", "info", "age");
            // 再次查看
            System.out.println("\n-------再次查看001--------\n");
            getData(tableName, "001");

            System.out.println("\n-------删除行001--------\n");
            // 删除行
            deleteRow(tableName, "001");
            getData(tableName, "001");
            System.out.println("\n-------查看数据002--------\n");
            getData(tableName, "002");

            System.out.println("\n-------删除表--------\n");
            // 删除表
            deleteTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
