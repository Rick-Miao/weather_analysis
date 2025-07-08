package edu.jit.weather_analysis.repository;

import edu.jit.weather_analysis.entity.WeatherWritable;
import edu.jit.weather_analysis.hbase.HBaseUtils;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 统计各个天气站的天气汇总仓库
 *
 * @author 缪彭哲
 */
public class WeatherStationSummaryRepository {
    private static class SummaryMapper extends TableMapper<Text, WeatherWritable> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, WeatherWritable>.Context context) throws IOException, InterruptedException {
            // 从hbase表中读取一行数据
            byte[] fm = Bytes.toBytes("info");
            byte[] column1 = Bytes.toBytes("precipitation");
            byte[] column2 = Bytes.toBytes("maxTemperature");
            byte[] column3 = Bytes.toBytes("minTemperature");
            byte[] column4 = Bytes.toBytes("avgTemperature");
            // 行键
            String rowkey = Bytes.toString(value.getRow());
            String code = rowkey.split("_")[0];
            // 值
            double precipitation = Bytes.toDouble(value.getValue(fm, column1));
            double maxTemperature = Bytes.toDouble(value.getValue(fm, column2));
            double minTemperature = Bytes.toDouble(value.getValue(fm, column3));
            double avgTemperature = Bytes.toDouble(value.getValue(fm, column4));
            // 实例化对象
            WeatherWritable weather = new WeatherWritable(code, "", precipitation, maxTemperature, minTemperature, avgTemperature);
            // 输出 ("code",w)
            context.write(new Text(code), weather);
        }
    }

    private static class SummaryReducer extends TableReducer<Text, WeatherWritable, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<WeatherWritable> values, Reducer<Text, WeatherWritable, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
            // 数据汇总: 写入hbase表中
            WeatherWritable weatherWritable = new WeatherWritable();
            weatherWritable.setMaxTemperature(0);
            weatherWritable.setMinTemperature(Double.MAX_VALUE);
            int count = 0;
            for (WeatherWritable w : values) {
                // 累加降水量
                weatherWritable.setPrecipitation(weatherWritable.getPrecipitation() + w.getPrecipitation());
                // 求最高温度
                weatherWritable.setMaxTemperature(Math.max(weatherWritable.getMaxTemperature(), w.getMaxTemperature()));
                // 求最低温度
                weatherWritable.setMinTemperature(Math.min(weatherWritable.getMinTemperature(), w.getMinTemperature()));
                // 累加平均气温
                weatherWritable.setAvgTemperature(weatherWritable.getAvgTemperature() + w.getAvgTemperature());
                count++;
            }
            // code date
            weatherWritable.setCode(key.toString());
            // 计算平均温度
            weatherWritable.setAvgTemperature(weatherWritable.getAvgTemperature() / count);
            // 构建行键 列簇 列 值
            byte[] rk = Bytes.toBytes(weatherWritable.getCode());
            byte[] cf = Bytes.toBytes("info");
            byte[] column1 = Bytes.toBytes("precipitation");
            byte[] column2 = Bytes.toBytes("maxTemperature");
            byte[] column3 = Bytes.toBytes("minTemperature");
            byte[] column4 = Bytes.toBytes("avgTemperature");
            byte[] value1 = Bytes.toBytes(weatherWritable.getPrecipitation());
            byte[] value2 = Bytes.toBytes(weatherWritable.getMaxTemperature());
            byte[] value3 = Bytes.toBytes(weatherWritable.getMinTemperature());
            byte[] value4 = Bytes.toBytes(weatherWritable.getAvgTemperature());
            // 构建Put
            Put put = new Put(rk);
            put.addColumn(cf, column1, value1);
            put.addColumn(cf, column2, value2);
            put.addColumn(cf, column3, value3);
            put.addColumn(cf, column4, value4);
            // 输出
            context.write(NullWritable.get(), put);
        }
    }

    public static void summary() {
        try {
            // 输入表
            String inputTableName = "weather_all";
            Table inputTable = HBaseUtils.getTable(inputTableName);
            Scan scan = new Scan();
            byte[] fm = Bytes.toBytes("info");
            byte[] column1 = Bytes.toBytes("precipitation");
            byte[] column2 = Bytes.toBytes("maxTemperature");
            byte[] column3 = Bytes.toBytes("minTemperature");
            byte[] column4 = Bytes.toBytes("avgTemperature");
            scan.addFamily(fm);
            scan.addColumn(fm, column1);
            scan.addColumn(fm, column2);
            scan.addColumn(fm, column3);
            scan.addColumn(fm, column4);
            // 输出表
            String outputTableName = "weather_all_summary";
            if (HBaseUtils.getTable(outputTableName).getScanner(new Scan()).next() != null) {
                return;
            }
            HBaseUtils.createTable(outputTableName, "info", false);
            // 创建job
            Job job = Job.getInstance(HBaseUtils.getConf(), "summaryAll");
            // 设置mapper
            TableMapReduceUtil.initTableMapperJob(inputTableName, scan, SummaryMapper.class, Text.class, WeatherWritable.class, job);
            // 设置reducer
            TableMapReduceUtil.initTableReducerJob(outputTableName, SummaryReducer.class, job);
            // 运行
            boolean success = job.waitForCompletion(true);
            System.out.println(success ? "成功" : "失败");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static List<WeatherWritable> getSummaryAll() {
        List<WeatherWritable> weathers = new ArrayList<>();
        try {
            Table table = HBaseUtils.getTable("weather_all_summary");
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            // 遍历
            for (Result result : scanner) {
                // 读取值
                String rowKey = Bytes.toString(result.getRow());
                double precipitation = Bytes.toDouble(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("precipitation")));
                double maxTemperature = Bytes.toDouble(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("maxTemperature")));
                double minTemperature = Bytes.toDouble(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("minTemperature")));
                double avgTemperature = Bytes.toDouble(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("avgTemperature")));
                // 实例化对象
                WeatherWritable weather = new WeatherWritable(rowKey,"", precipitation, maxTemperature, minTemperature, avgTemperature);
                // 添加到集合
                weathers.add(weather);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return weathers;
    }
}
