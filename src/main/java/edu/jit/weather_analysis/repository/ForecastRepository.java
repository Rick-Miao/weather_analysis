package edu.jit.weather_analysis.repository;

import edu.jit.weather_analysis.entity.WeatherWritable;
import edu.jit.weather_analysis.hbase.HBaseUtils;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

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
            String rowKey = Bytes.toString(value.getRow());
            String code = rowKey.split("_")[0];
            String date = rowKey.split("_")[1];
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
                String day_month = sdf.format(calendar.getTime());
                // 与历史上的今天对比
                if (date.startsWith(day_month)) {
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
            weather.setPrecipitation(precipitation);
            weather.setMaxTemperature(maxTemperature);
            weather.setMinTemperature(minTemperature);
            weather.setAvgTemperature(avgTemperature);
            // 输出 ("code_dd/MM",w)
            context.write(new Text(code + "_" + date.substring(0, date.lastIndexOf("/"))), weather);
        }
    }

    private static class ForecastReducer extends TableReducer<Text, WeatherWritable, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<WeatherWritable> values, Reducer<Text, WeatherWritable, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
            // 历史上的今天 数据计算平均值
            // 今天
            WeatherWritable weatherWritable = new WeatherWritable();
            int count = 0;
            for (WeatherWritable w : values) {
                weatherWritable.setCode(w.getCode());
                weatherWritable.setDate(w.getDate());
                weatherWritable.setPrecipitation(weatherWritable.getPrecipitation() + w.getPrecipitation());
                weatherWritable.setMaxTemperature(weatherWritable.getMaxTemperature() + w.getMaxTemperature());
                weatherWritable.setMinTemperature(weatherWritable.getMinTemperature() + w.getMinTemperature());
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

    public static void forecast7days() {
        try {
            String todayDate = new SimpleDateFormat("dd/MM/yyyy").format(new Date());
            if (hasTodayForecast(todayDate)) {
                System.out.println("今日预测数据已存在，跳过执行");
                return;
            }
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

    public static List<WeatherWritable> get7days() {
        List<WeatherWritable> weathers = new ArrayList<>();
        try {
            Table table = HBaseUtils.getTable("forecast");
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
                WeatherWritable weather = new WeatherWritable(rowKey.split("_")[0], rowKey.split("_")[1], precipitation, maxTemperature, minTemperature, avgTemperature);
                // 添加到集合
                weathers.add(weather);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return weathers;
    }

    private static boolean hasTodayForecast(String todayDate) throws IOException {
        try (Table table = HBaseUtils.getTable("forecast")) {
            Scan scan = new Scan();
            // 使用正则匹配行键以 "_dd/MM/yyyy" 结尾的行
            String regex = ".*_" + Pattern.quote(todayDate); // 转义特殊字符
            Filter filter = new RowFilter(
                    CompareFilter.CompareOp.EQUAL,
                    new RegexStringComparator(regex)
            );
            scan.setFilter(filter);
            scan.setLimit(1); // 优化：找到1条即返回

            return table.getScanner(scan).iterator().hasNext();
        }
    }
}
