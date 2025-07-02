package edu.jit.weather_analysis.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * 首页控制器
 *
 * @author 缪彭哲
 */
@Controller
public class IndexController {
    @GetMapping(value = {"", "/", "/index"})
    public String getIndex() {
        // 原理: spring.thymeleaf.prefix = classpath:/templates/
        // 文件名
        // spring.thymeleaf.suffix = .html
        // 返回: 前缀 + 文件名 + 后缀
        return "index";
    }
}
