package edu.jit.weather_analysis.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * 用户控制器
 *
 * @author 缪彭哲
 */
@Controller
public class UserController {
    @GetMapping(value={"register"})
    public String getRegister() {
        return "register";
    }
}
