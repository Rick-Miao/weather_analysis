package edu.jit.weather_analysis.repository;

import edu.jit.weather_analysis.entity.WeatherWritable;
import edu.jit.weather_analysis.hbase.HBaseUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/**
 * 导入数据仓库类s
 *
 * @author 缪彭哲
 */
public class ImportDataRepository {
    private static class ImportMapper extends Mapper<LongWritable, Text, Text, WeatherWritable> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, WeatherWritable>.Context context) throws IOException, InterruptedException {
            // 数据拆分: 获取数据并解析
            String line = value.toString();
            if (StringUtils.isEmpty(line)) {
                return;
            }
            // 跳过标题栏
            if (line.startsWith("Estacao")) {
                return;
            }
            // 巴西全国数据: 只选择巴西利亚(83377)
            if (!line.startsWith("83377")) {
                return;
            }
            // 验证数据是否完整
            String[] fields = line.split(";", 19);
            if (fields.length != 19) {
                return;
            }
            // 提取数据
            String code = fields[0];
            String date = fields[1];
            double precipitation = Double.parseDouble(StringUtils.isEmpty(fields[3]) ? "0" : fields[3]);
            double maxTemperature = Double.parseDouble(StringUtils.isEmpty(fields[6]) ? "0" : fields[6]);
            double minTemperature = Double.parseDouble(StringUtils.isEmpty(fields[7]) ? "0" : fields[7]);
            double avgTemperature = Double.parseDouble(StringUtils.isEmpty(fields[16]) ? "0" : fields[16]);

            // 实例化对象
            WeatherWritable weatherWritable = new WeatherWritable(code, date, precipitation, maxTemperature, minTemperature, avgTemperature);
            // 输出: "83377_09/03/2019",weatherWritable
            context.write(new Text(code + "_" + date), weatherWritable);
        }
    }

    private static class ImportReducer extends TableReducer<Text, WeatherWritable, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<WeatherWritable> values, Reducer<Text, WeatherWritable, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
            // 数据汇总: 写入hbase表中
            // 三行合一
            WeatherWritable weatherWritable = new WeatherWritable();
            for (WeatherWritable w : values) {
                weatherWritable.setCode(w.getCode());
                weatherWritable.setDate(w.getDate());
                weatherWritable.setPrecipitation(weatherWritable.getPrecipitation() + w.getPrecipitation());
                weatherWritable.setMaxTemperature(weatherWritable.getMaxTemperature() + w.getMaxTemperature());
                weatherWritable.setMinTemperature(weatherWritable.getMinTemperature() + w.getMinTemperature());
                weatherWritable.setAvgTemperature(weatherWritable.getAvgTemperature() + w.getAvgTemperature());
            }
            // 构建行键 列簇 列 值
            byte[] rk = Bytes.toBytes(weatherWritable.getCode() + "_" + weatherWritable.getDate());
            byte[] cf = Bytes.toBytes("info");
            byte[] column1 = Bytes.toBytes("precipitation");
            byte[] value1 = Bytes.toBytes(weatherWritable.getPrecipitation());
            byte[] column2 = Bytes.toBytes("maxTemperature");
            byte[] value2 = Bytes.toBytes(weatherWritable.getMaxTemperature());
            byte[] column3 = Bytes.toBytes("minTemperature");
            byte[] value3 = Bytes.toBytes(weatherWritable.getMinTemperature());
            byte[] column4 = Bytes.toBytes("avgTemperature");
            byte[] value4 = Bytes.toBytes(weatherWritable.getAvgTemperature());
            // 构建Put
            Put put = new Put(rk);
            put.addColumn(cf, column1, value1);
            put.addColumn(cf, column2, value2);
            put.addColumn(cf, column3, value3);
            put.addColumn(cf, column4, value4);
            // 写入hbase表中
            context.write(NullWritable.get(), put);
        }
    }

    public static void run() {
        try {
            // 表名
            String tableName = "weather";
            // 创建表
            HBaseUtils.createTable(tableName, "info", true);
            // 写入的hbase表
            HBaseUtils.getConf().set(TableOutputFormat.OUTPUT_TABLE, tableName);
            // 创建job
            Job job = Job.getInstance(HBaseUtils.getConf(), "ImportData");
            // 设置输入
            job.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.addInputPath(job, new Path("hdfs://master:9000/brazil_weather/*"));
            // 设置mapper
            job.setMapperClass(ImportMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(WeatherWritable.class);
            // 设置reducer
            job.setReducerClass(ImportReducer.class);
            // 设置输出
            job.setOutputFormatClass(TableOutputFormat.class);
            // 运行
            boolean success = job.waitForCompletion(true);
            // 输出提示信息
            System.out.println(success ? "成功" : "失败");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}