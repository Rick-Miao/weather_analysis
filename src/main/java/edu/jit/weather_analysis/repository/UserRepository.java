package edu.jit.weather_analysis.repository;

import edu.jit.weather_analysis.entity.User;
import edu.jit.weather_analysis.hbase.HBaseUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * 用户仓库类
 *
 * @author 缪彭哲
 */
@Service
public class UserRepository {
    public void register(User user) {
        // 用户注册
        // 创建表User
        HBaseUtils.createTable("user", "info", false);
        // 添加数据
        byte[] rk = Bytes.toBytes(user.getUsername());
        byte[] family = Bytes.toBytes("info");
        byte[] col1 = Bytes.toBytes("password");
        byte[] col2 = Bytes.toBytes("nickname");
        byte[] col3 = Bytes.toBytes("phone");
        byte[] col4 = Bytes.toBytes("email");
        byte[] val1 = Bytes.toBytes(user.getPassword());
        byte[] val2 = Bytes.toBytes(user.getNickname());
        byte[] val3 = Bytes.toBytes(user.getPhone());
        byte[] val4 = Bytes.toBytes(user.getEmail());
        Put put = new Put(rk);
        put.addColumn(family, col1, val1);
        put.addColumn(family, col2, val2);
        put.addColumn(family, col3, val3);
        put.addColumn(family, col4, val4);
        Table table = HBaseUtils.getTable("user");
        try {
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
