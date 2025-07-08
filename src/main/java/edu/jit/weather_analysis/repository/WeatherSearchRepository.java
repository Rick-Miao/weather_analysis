package edu.jit.weather_analysis.repository;

import edu.jit.weather_analysis.entity.WeatherWritable;
import edu.jit.weather_analysis.hbase.HBaseUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Component;

/**
 * 天气查询仓库类
 *
 * @author 缪彭哲
 */
@Component
public class WeatherSearchRepository {
    public WeatherWritable search(String code, String date) {
        try {
            // 获取表
            Table table = HBaseUtils.getTable("weather_all");
            // 定义行键
            String rowKey = code + "_" + date;
            byte[] rk = Bytes.toBytes(rowKey);
            // 定义Get
            Get get = new Get(rk);
            // 执行查询
            Result result = table.get(get);
            // 实例化对象
            WeatherWritable weather = new WeatherWritable();
            weather.setCode(code);
            weather.setDate(date);
            weather.setPrecipitation(Bytes.toDouble(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("precipitation"))));
            weather.setMaxTemperature(Bytes.toDouble(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("maxTemperature"))));
            weather.setMinTemperature(Bytes.toDouble(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("minTemperature"))));
            weather.setAvgTemperature(Bytes.toDouble(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("avgTemperature"))));
            return weather;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new WeatherWritable("", "", 0, 0, 0, 0);
    }
}
