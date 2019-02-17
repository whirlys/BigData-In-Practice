package com.whirly.coprocessor.endpoint;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.whirly.coprocessor.endpoint.GetRowCount.getRowCountRequest;
import com.whirly.coprocessor.endpoint.GetRowCount.getRowCountResponse;
import com.whirly.coprocessor.endpoint.GetRowCount.hbaseEndPointTestService;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: HbaseExamples
 * @description: hbase Endpoint
 * https://blog.csdn.net/carl810224/article/details/52224441
 * @author: 赖键锋
 * @create: 2019-02-17 15:37
 **/
public class TestRowCountEndPoint extends
        hbaseEndPointTestService implements Coprocessor,
        CoprocessorService {

    // 单个region的上下文环境信息
    private RegionCoprocessorEnvironment envi;

    // rpc服务，返回本身即可，因为此类实例就是一个服务实现
    @Override
    public Service getService() {
        return this;
    }

    // 协处理器是运行于region中的，每一个region都会加载协处理器
    // 这个方法会在regionserver打开region时候执行（还没有真正打开）
    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        // 需要检查当前环境是否在region上
        if (env instanceof RegionCoprocessorEnvironment) {
            this.envi = (RegionCoprocessorEnvironment) env;

        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }

    }

    // 这个方法会在regionserver关闭region时候执行（还没有真正关闭）
    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        // nothing to do

    }

    // 服务端（每一个region上）的接口实现方法
    // 第一个参数是固定的，其余的request参数和response参数是proto接口文件中指明的。
    @Override
    public void getRowCount(RpcController controller,
                            getRowCountRequest request,
                            RpcCallback<getRowCountResponse> done) {
        // 单个region上的计算结果值
        int result = 0;

        // 定义返回response
        getRowCountResponse.Builder responseBuilder = getRowCountResponse.newBuilder();
        // 进行行数统计
        InternalScanner scanner = null;
        try {
            Scan scan = new Scan();
            scanner = this.envi.getRegion().getScanner(scan);
            List<Cell> results = new ArrayList<Cell>();
            boolean hasMore = false;

            do {
                hasMore = scanner.next(results);
                result++;
            } while (hasMore);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (IOException ignored) {
                    // nothing to do
                }
            }
        }

        responseBuilder.setRowCount(result);
        done.run(responseBuilder.build());
        return;

    }
}
