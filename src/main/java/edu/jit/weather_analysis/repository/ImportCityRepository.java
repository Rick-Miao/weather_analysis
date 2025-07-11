package edu.jit.weather_analysis.repository;

import edu.jit.weather_analysis.entity.CityWritable;
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
 * 导入城市编号仓库类
 *
 * @author 缪彭哲
 */
public class ImportCityRepository {
    private static class ImportMapper extends Mapper<LongWritable, Text, Text, CityWritable> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, CityWritable>.Context context) throws IOException, InterruptedException {
            // 数据拆分: 获取数据并解析
            String line = value.toString().replace("\"", "").trim();
            if (StringUtils.isEmpty(line)) {
                return;
            }
            // 跳过标题栏
            if (line.startsWith("Nome")) {
                return;
            }
            // 验证数据是否完整
            String[] fields = line.split(";", 7);
            if (fields.length != 7) {
                return;
            }
            // 提取数据
            String name = fields[0].trim();
            String code = fields[1].trim();
            double latitude = Double.parseDouble(StringUtils.isEmpty(fields[2].trim()) ? "0" : fields[2]);
            double longitude = Double.parseDouble(StringUtils.isEmpty(fields[3].trim()) ? "0" : fields[3]);
            double altitude = Double.parseDouble(StringUtils.isEmpty(fields[4].trim()) ? "0" : fields[4]);
            // 创建City对象
            CityWritable city = new CityWritable(code, name, latitude, longitude, altitude);
            // 输出: "83377", city
            context.write(new Text(code), city);
        }
    }


    private static class ImportReducer extends TableReducer<Text, CityWritable, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<CityWritable> values, Reducer<Text, CityWritable, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
            // 数据汇总: 写入hbase表中
            CityWritable city = values.iterator().next();
            Put put = new Put(Bytes.toBytes(city.getCode()));
            put.addColumn(
                    Bytes.toBytes("info"),
                    Bytes.toBytes("name"),
                    Bytes.toBytes(city.getName())
            );
            put.addColumn(
                    Bytes.toBytes("info"),
                    Bytes.toBytes("latitude"),
                    Bytes.toBytes(city.getLatitude())
            );
            put.addColumn(
                    Bytes.toBytes("info"),
                    Bytes.toBytes("longitude"),
                    Bytes.toBytes(city.getLongitude())
            );
            put.addColumn(
                    Bytes.toBytes("info"),
                    Bytes.toBytes("altitude"),
                    Bytes.toBytes(city.getAltitude())
            );
            // 写入hbase表中
            context.write(NullWritable.get(), put);
        }
    }

    public static void main(String[] args) {
        try {

            // 表名
            String tableName = "city";
            // 创建表
            HBaseUtils.createTable(tableName, "info", true);
            // 写入的hbase表
            HBaseUtils.getConf().set(TableOutputFormat.OUTPUT_TABLE, tableName);
            // 创建job
            Job job = Job.getInstance(HBaseUtils.getConf(), "ImportCityData");
            job.setJarByClass(ImportCityRepository.class);
            // 设置输入
            job.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.addInputPath(job, new Path("hdfs://master:9000/brazil_weather/weather_stations_codes.csv"));
            // 设置mapper
            job.setMapperClass(ImportMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(CityWritable.class);
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