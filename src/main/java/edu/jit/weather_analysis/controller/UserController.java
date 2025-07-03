package edu.jit.weather_analysis.controller;

import edu.jit.weather_analysis.entity.User;
import edu.jit.weather_analysis.repository.UserRepository;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;

import javax.annotation.Resource;

/**
 * 用户控制器
 *
 * @author 缪彭哲
 */
@Controller
public class UserController {
    @Resource
    private UserRepository userRepository;

    @GetMapping(value = {"register"})
    public String getRegister() {
        return "register";
    }

    @PostMapping(value = {"register"})
    public String register(User user) {
        // 实现注册
        userRepository.register(user);
        // 页面重定向
        return "redirect:/login";
    }

    @GetMapping(value = {"login"})
    public String getLogin() {
        return "login";
    }
}
