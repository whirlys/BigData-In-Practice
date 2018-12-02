import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @program: customizePartition
 * @description: 自定义分区，把各个部门的数据分发到各自的reduce task上
 * @author: 赖键锋
 * @create: 2018-12-02 16:50
 **/
public class JiduPartitioner<K, V> extends Partitioner<K, V> {

    /**
     * 自定义partition的数量需要和reduce task数量保持一致
     */
    @Override
    public int getPartition(K key, V value, int numPartitions) {
        String dname = key.toString().trim();
        int index = 4;
        switch (dname) {
            case "研发部门":
                index = 0;
                break;
            case "测试部门":
                index = 1;
                break;
            case "硬件部门":
                index = 2;
                break;
            case "销售部门":
                index = 3;
                break;
        }
        return index;
    }
}
