package edu.jit.weather_analysis.controller;

import edu.jit.weather_analysis.entity.WeatherWritable;
import edu.jit.weather_analysis.repository.ForecastRepository;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.ui.Model;


import java.util.List;

/**
 * 首页控制器
 *
 * @author 缪彭哲
 */
@Controller
public class IndexController {
    @GetMapping(value = {"", "/", "/index"})
    public String getIndex(Model model) {
        // 原理: spring.thymeleaf.prefix = classpath:/templates/
        // 文件名
        // spring.thymeleaf.suffix = .html
        // 返回: 前缀 + 文件名 + 后缀
        // 调用方法进行预测
        ForecastRepository.forecast7days();
        // 调用方法返回7天预测数据
        List<WeatherWritable> weathers = ForecastRepository.get7days();
        // 传递给前端页面
        model.addAttribute("weathers", weathers);

        return "index";
    }
}
