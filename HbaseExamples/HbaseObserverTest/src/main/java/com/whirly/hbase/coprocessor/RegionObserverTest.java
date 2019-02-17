package com.whirly.hbase.coprocessor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @program: HbaseExamples
 * @description: RegionObserver 协处理器
 * https://blog.csdn.net/liu16659/article/details/80738396
 * @author: 赖键锋
 * @create: 2019-02-17 00:43
 **/
public class RegionObserverTest extends BaseRegionObserver {
    private byte[] columnFamily = Bytes.toBytes("cf");
    private byte[] countCol = Bytes.toBytes("countCol");
    private byte[] unDeleteCol = Bytes.toBytes("unDeleteCol");
    private RegionCoprocessorEnvironment environment;

    // regionserver 打开region前执行
    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        environment = (RegionCoprocessorEnvironment) e;
    }

    // RegionServer关闭region前调用
    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {

    }

    /**
     * 1. cf:countCol 进行累加操作。 每次插入的时候都要与之前的值进行相加
     */
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit,
                       Durability durability) throws IOException {
        if (put.has(columnFamily, countCol)) {
            //获取old countcol value
            Result rs = e.getEnvironment().getRegion().get(new Get(put.getRow()));
            int oldNum = 0;
            for (Cell cell : rs.rawCells()) {
                if (CellUtil.matchingColumn(cell, columnFamily, countCol)) {
                    oldNum = Integer.valueOf(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }

            //获取new countcol value
            List<Cell> cells = put.get(columnFamily, countCol);
            int newNum = 0;
            for (Cell cell : cells) {
                if (CellUtil.matchingColumn(cell, columnFamily, countCol)) {
                    newNum = Integer.valueOf(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }

            //sum AND update Put实例
            put.addColumn(columnFamily, countCol, Bytes.toBytes(String.valueOf(oldNum + newNum)));
        }
    }

    /**
     * 2. 不能直接删除unDeleteCol    删除countCol的时候将unDeleteCol一同删除
     */
    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete,
                          WALEdit edit,
                          Durability durability) throws IOException {
        //判断是否操作cf列族
        List<Cell> cells = delete.getFamilyCellMap().get(columnFamily);
        if (cells == null || cells.size() == 0) {
            return;
        }

        boolean deleteFlag = false;
        for (Cell cell : cells) {
            byte[] qualifier = CellUtil.cloneQualifier(cell);

            if (Arrays.equals(qualifier, unDeleteCol)) {
                throw new IOException("can not delete unDel column");
            }

            if (Arrays.equals(qualifier, countCol)) {
                deleteFlag = true;
            }
        }

        if (deleteFlag) {
            delete.addColumn(columnFamily, unDeleteCol);
        }
    }
}
