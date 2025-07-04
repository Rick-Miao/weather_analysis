package edu.jit.weather_analysis.entity;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 天气大数据实体类
 *
 * @author 缪彭哲
 */
public class WeatherWritable implements WritableComparable<WeatherWritable> {
    private String code;
    private String date;
    private double precipitation;
    private double maxTemperature;
    private double minTemperature;
    private double avgTemperature;

    @Override
    public int compareTo(WeatherWritable other) {
        // 对象比较, 也是默认排序
        if (null == other) {
            return 1;
        }
        // 比较城市
        if (this.code.compareTo(other.code) != 0) {
            return this.code.compareTo(other.code);
        }
        // 比较日期
        return this.date.compareTo(other.date);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // hadoop序列化
        out.writeUTF(code);
        out.writeUTF(date);
        out.writeDouble(precipitation);
        out.writeDouble(maxTemperature);
        out.writeDouble(minTemperature);
        out.writeDouble(avgTemperature);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // hadoop反序列化
        code = in.readUTF();
        date = in.readUTF();
        precipitation = in.readDouble();
        maxTemperature = in.readDouble();
        minTemperature = in.readDouble();
        avgTemperature = in.readDouble();
    }

    public WeatherWritable() {
    }

    public WeatherWritable(String code, String date, double precipitation, double maxTemperature, double minTemperature, double avgTemperature) {
        this.code = code;
        this.date = date;
        this.precipitation = precipitation;
        this.maxTemperature = maxTemperature;
        this.minTemperature = minTemperature;
        this.avgTemperature = avgTemperature;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public double getPrecipitation() {
        return precipitation;
    }

    public void setPrecipitation(double precipitation) {
        this.precipitation = precipitation;
    }

    public double getMaxTemperature() {
        return maxTemperature;
    }

    public void setMaxTemperature(double maxTemperature) {
        this.maxTemperature = maxTemperature;
    }

    public double getMinTemperature() {
        return minTemperature;
    }

    public void setMinTemperature(double minTemperature) {
        this.minTemperature = minTemperature;
    }

    public double getAvgTemperature() {
        return avgTemperature;
    }

    public void setAvgTemperature(double avgTemperature) {
        this.avgTemperature = avgTemperature;
    }
}
