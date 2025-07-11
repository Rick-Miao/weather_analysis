package edu.jit.weather_analysis.repository;

import edu.jit.weather_analysis.entity.CityWritable;
import edu.jit.weather_analysis.hbase.HBaseUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * 城市编码仓库类
 *
 * @author 缪彭哲
 */
@Component
public class CityCodeRepository {
    public static Map<String, CityWritable> getAllCityDetails() {
        Map<String, CityWritable> cityMap = new HashMap<>();
        try (Table table = HBaseUtils.getTable("city")) {  // 从 city 表查询
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("latitude"));
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("longitude"));
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("altitude"));
            scan.setCaching(1000);

            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    String code = Bytes.toString(result.getRow()); // 行键 = 城市编号
                    String name = Bytes.toString(result.getValue(
                            Bytes.toBytes("info"),
                            Bytes.toBytes("name")
                    )); // 城市名称
                    double latitude = Bytes.toDouble(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("latitude")));
                    double longitude = Bytes.toDouble(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("longitude")));
                    double altitude = Bytes.toDouble(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("altitude")));

                    cityMap.put(code, new CityWritable(code, name, latitude, longitude, altitude));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return cityMap;
    }
}
