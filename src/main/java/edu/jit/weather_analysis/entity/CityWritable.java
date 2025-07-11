package edu.jit.weather_analysis.entity;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 城市实体类
 *
 * @author 缪彭哲
 */
public class CityWritable implements WritableComparable<CityWritable> {
    private String code;
    private String name;
    private double latitude;
    private double longitude;
    private double altitude;

    public CityWritable() {
    }

    public CityWritable(String code, String name, double latitude, double longitude, double altitude) {
        this.code = code;
        this.name = name;
        this.latitude = latitude;
        this.longitude = longitude;
        this.altitude = altitude;
    }

    @Override
    public int compareTo(CityWritable other) {
        // 对象比较, 也是默认排序
        if (null == other) {
            return 1;
        }
        // 比较城市
        return this.code.compareTo(other.code);
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(code);
        out.writeUTF(name);
        out.writeDouble(latitude);
        out.writeDouble(longitude);
        out.writeDouble(altitude);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        code = in.readUTF();
        name = in.readUTF();
        latitude = in.readDouble();
        longitude = in.readDouble();
        altitude = in.readDouble();
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getAltitude() {
        return altitude;
    }

    public void setAltitude(double altitude) {
        this.altitude = altitude;
    }
}
