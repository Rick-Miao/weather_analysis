package edu.jit.weather_analysis.controller;

import edu.jit.weather_analysis.entity.WeatherWritable;
import edu.jit.weather_analysis.repository.ForecastRepository;
import edu.jit.weather_analysis.repository.ImportDataRepository;
import edu.jit.weather_analysis.repository.SummaryRepository;
import edu.jit.weather_analysis.repository.WeatherSearchRepository;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

import javax.annotation.Resource;
import java.util.List;

/**
 * 天气控制器
 *
 * @author 缪彭哲
 */
@Controller
public class WeatherController {
    @Resource
    private WeatherSearchRepository weatherSearchRepository;

    @GetMapping("/importData")
    public String importData() {
        // 从hdfs导入数据至hbase表weather中
        ImportDataRepository.run();
        return "index";
    }

    @GetMapping("/search")
    public String search() {
        return "search";
    }

    @GetMapping("/searchWeather")
    public String search(String code, String date, Model model) {
        // 调用查询方法
        WeatherWritable weather = weatherSearchRepository.search(code, date);
        // 传递数据给前端页面
        model.addAttribute("weather", weather);
        return "search";
    }

    @GetMapping("/summary")
    public String getSummary(Model model) {
        // 调用统计每年的天气方法
        SummaryRepository.summary();
        // 调用方法返回所有每年的天气数据
        List<WeatherWritable> weathers = SummaryRepository.getSummaryAll();
        // 传递给前端页面
        model.addAttribute("weathers", weathers);
        // 跳转到统计页面
        return "summary";
    }

    @GetMapping("/forecast7days")
    public String getForecast7days(Model model) {
        // 调用方法进行预测
        ForecastRepository.forecast7days();
        // 调用方法返回7天预测数据
        List<WeatherWritable> weathers = ForecastRepository.get7days();
        // 传递给前端页面
        model.addAttribute("weathers", weathers);
        // 跳转到统计页面
        return "forecast7days";
    }
}
