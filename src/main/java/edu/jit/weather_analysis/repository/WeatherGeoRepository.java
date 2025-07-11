package edu.jit.weather_analysis.repository;

import edu.jit.weather_analysis.entity.CityWritable;
import edu.jit.weather_analysis.entity.WeatherWritable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author 缪彭哲
 */
public class WeatherGeoRepository {
    public static List<Map<String, Object>> getWeatherWithGeoData() {
        List<Map<String, Object>> result = new ArrayList<>();

        try {
            // 1. 获取所有城市的地理信息
            Map<String, CityWritable> cities = CityCodeRepository.getAllCityDetails();

            // 2. 获取天气汇总数据
            List<WeatherWritable> weatherList = WeatherStationSummaryRepository.getSummaryAll();

            // 3. 关联数据
            for (WeatherWritable weather : weatherList) {
                CityWritable city = cities.get(weather.getCode());

                if (city != null) {
                    Map<String, Object> item = new HashMap<>();
                    item.put("name", city.getName());
                    item.put("code", city.getCode());
                    item.put("value", new double[]{
                            city.getLongitude(), // 经度
                            city.getLatitude(),   // 纬度
                            weather.getPrecipitation() // 降水量
                    });
                    item.put("maxTemperature", weather.getMaxTemperature());
                    item.put("minTemperature", weather.getMinTemperature());
                    item.put("avgTemperature", weather.getAvgTemperature());
                    item.put("city", city);
                    item.put("weather", weather);

                    result.add(item);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("获取天气地理数据失败", e);
        }

        return result;
    }
}
