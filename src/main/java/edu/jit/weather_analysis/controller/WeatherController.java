package edu.jit.weather_analysis.controller;

import edu.jit.weather_analysis.entity.WeatherWritable;
import edu.jit.weather_analysis.repository.ImportDataRepository;
import edu.jit.weather_analysis.repository.WeatherSearchRepository;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

import javax.annotation.Resource;

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
}
