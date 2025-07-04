package edu.jit.weather_analysis.controller;

import edu.jit.weather_analysis.repository.ImportDataRepository;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * 天气控制器
 *
 * @author 缪彭哲
 */
@Controller
public class WeatherController {
    @GetMapping("/importData")
    public String importData() {
        // 从hdfs导入数据至hbase表weather中
        ImportDataRepository.run();
        return "index";
    }
}
