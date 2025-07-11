package edu.jit.weather_analysis.repository;

import edu.jit.weather_analysis.hbase.HBaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 数据量统计仓库
 *
 * @author 缪彭哲
 */
public class DataCountRepository {
    public static class CountMapper extends TableMapper<Text, IntWritable> {
        private final static IntWritable ONE = new IntWritable(1);
        private Text code = new Text();

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result,
                Text, IntWritable>.Context context) throws IOException, InterruptedException {
            String rowKey = Bytes.toString(key.get());
            String[] parts = rowKey.split("_");
            if (parts.length >= 1) {
                code.set(parts[0]);
                context.write(code, ONE);
            }
        }
    }

    public static class CountReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable,
                ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(
                    Bytes.toBytes("info"),
                    Bytes.toBytes("count"),
                    Bytes.toBytes(sum)
            );
            context.write(null, put);
        }
    }

    public static void run() {
        try {
            Job job = Job.getInstance(HBaseUtils.getConf(), "count");
            job.setJarByClass(DataCountRepository.class);
            Scan scan = new Scan();
            TableMapReduceUtil.initTableMapperJob(
                    "weather_all",
                    scan,               // 扫描配置
                    CountMapper.class,  // Mapper类
                    Text.class,         // Mapper输出key类型
                    IntWritable.class,  // Mapper输出value类型
                    job
            );
            TableMapReduceUtil.initTableReducerJob(
                    "count",    // 输出表名
                    CountReducer.class, // Reducer类
                    job
            );
            HBaseUtils.createTable("count", "info", false);
            boolean success = job.waitForCompletion(true);
            System.out.println(success ? "成功" : "失败");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Map<String, Integer> getAllCounts() {
        Map<String, Integer> result = new HashMap<>();
        Configuration config = HBaseUtils.getConf();
        try (Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(TableName.valueOf("count"))) {

            Scan scan = new Scan();
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result res : scanner) {
                    String rowKey = Bytes.toString(res.getRow());
                    byte[] countBytes = res.getValue(Bytes.toBytes("info"), Bytes.toBytes("count"));
                    if (countBytes != null) {
                        int count = Bytes.toInt(countBytes);
                        result.put(rowKey, count);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }
}
