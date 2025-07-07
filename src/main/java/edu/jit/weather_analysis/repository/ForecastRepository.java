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
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * 预测仓库类
 *
 * @author 缪彭哲
 */
public class ForecastRepository {
    private static class ForecastMapper extends TableMapper<Text, WeatherWritable> {
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
            String date = rowkey.split("_")[1];
            // 未来7天预测天气
            // 今天
            Calendar calendar = Calendar.getInstance();
            // 定义日期格式
            SimpleDateFormat sdf = new SimpleDateFormat("dd/MM");
            boolean flag = false;
            WeatherWritable weather = new WeatherWritable();
            weather.setCode(code);
            calendar.add(Calendar.DATE, -1);
            for (int i = 0; i < 7; i++) {
                // 加1天, 完整循环是7天, 包含今天
                calendar.add(Calendar.DATE, 1);
                // 日期格式转换
                String date_month = sdf.format(calendar.getTime());
                // 与历史上的今天对比
                if (date.startsWith(date_month)) {
                    flag = true;
                }
                // 只要是7天中的一天 就跳出循环
                if (flag) {
                    weather.setDate(new SimpleDateFormat("dd/MM/yyyy").format(calendar.getTime()));
                    break;
                }
            }
            // 不是7天的数据
            if (!flag) {
                return;
            }
            // 值
            double precipitation = Bytes.toDouble(value.getValue(fm, column1));
            double maxTemperature = Bytes.toDouble(value.getValue(fm, column2));
            double minTemperature = Bytes.toDouble(value.getValue(fm, column3));
            double avgTemperature = Bytes.toDouble(value.getValue(fm, column4));
            // 输出 ("code_dd/MM",w)
            context.write(new Text(code + "_" + date.substring(0, date.lastIndexOf("/"))), weather);
        }
    }

    private static class ForecastReducer extends TableReducer<Text, WeatherWritable, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<WeatherWritable> values, Reducer<Text, WeatherWritable, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
            // 历史上的今天 数据计算平均值
            // 今天
            Calendar calendar = Calendar.getInstance();
            WeatherWritable weatherWritable = new WeatherWritable();
            int count = 0;
            for (WeatherWritable w : values) {
                weatherWritable.setCode(w.getCode());
                weatherWritable.setDate(w.getDate());
                weatherWritable.setPrecipitation(weatherWritable.getPrecipitation() + w.getPrecipitation());
                weatherWritable.setMaxTemperature(Math.max(weatherWritable.getMaxTemperature(), w.getMaxTemperature()));
                weatherWritable.setMinTemperature(Math.min(weatherWritable.getMinTemperature(), w.getMinTemperature()));
                weatherWritable.setAvgTemperature(weatherWritable.getAvgTemperature() + w.getAvgTemperature());
                count++;
            }
            weatherWritable.setPrecipitation(weatherWritable.getPrecipitation() / count);
            weatherWritable.setMaxTemperature(weatherWritable.getMaxTemperature() / count);
            weatherWritable.setMinTemperature(weatherWritable.getMinTemperature() / count);
            weatherWritable.setAvgTemperature(weatherWritable.getAvgTemperature() / count);
            // 构建行键 列簇 列 值
            byte[] rk = Bytes.toBytes(weatherWritable.getCode() + "_" + weatherWritable.getDate());
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

    public static void main(String[] args) {
        try {
            // 输入表
            String inputTableName = "weather";
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
            String outputTableName = "forecast";
            // if (HBaseUtils.getTable(outputTableName).getScanner(new Scan()).next() != null) {
            //     return;
            // }
            HBaseUtils.createTable(outputTableName, "info", true);
            // 创建job
            Job job = Job.getInstance(HBaseUtils.getConf(), "forecast");
            // 设置mapper
            TableMapReduceUtil.initTableMapperJob(inputTableName, scan, ForecastMapper.class, Text.class, WeatherWritable.class, job);
            // 设置reducer
            TableMapReduceUtil.initTableReducerJob(outputTableName, ForecastReducer.class, job);
            // 运行
            boolean success = job.waitForCompletion(true);
            System.out.println(success ? "成功" : "失败");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
