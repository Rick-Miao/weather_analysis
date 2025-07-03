package edu.jit.weather_analysis.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase封装类
 *
 * @author 缪彭哲
 */
public class HBaseUtils {
    private static Configuration conf;
    private static Connection connection;
    private static Admin admin;

    static {
        try {
            conf = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void createTable(String tableName, String family, boolean isDrop) {
        try {
            TableName tn = TableName.valueOf(tableName);
            // 表存在, 删除, 重新创建
            if (admin.tableExists(tn) && isDrop) {
                admin.disableTable(tn);
                admin.deleteTable(tn);
            }
            // 表存在, 不删除, 不创建
            if (admin.tableExists(tn) && !isDrop) {
                return;
            }
            // 构建列簇结构
            byte[] fm = Bytes.toBytes(family);
            ColumnFamilyDescriptorBuilder cfdBuilder = ColumnFamilyDescriptorBuilder.newBuilder(fm);
            // 构建表结构
            TableDescriptorBuilder tbBuilder = TableDescriptorBuilder.newBuilder(tn);
            tbBuilder.setColumnFamily(cfdBuilder.build());
            // 创建表
            admin.createTable(tbBuilder.build());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Table getTable(String tableName) {
        try {
            TableName tn = TableName.valueOf(tableName);
            return connection.getTable(tn);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
