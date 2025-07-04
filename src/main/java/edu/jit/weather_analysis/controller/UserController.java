package edu.jit.weather_analysis.controller;

import edu.jit.weather_analysis.entity.User;
import edu.jit.weather_analysis.repository.UserRepository;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

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

    @PostMapping(value = {"login"})
    public String login(String username, String password, HttpSession session) {
        // 实现登录
        User user = userRepository.login(username, password);
        // 登录成功，页面重定向
        if (user != null) {
            // 存储用户信息
            session.setAttribute("user", user);
            return "redirect:/index";
        }
        // 登录失败, 移除用户信息
        session.removeAttribute("user");
        // 重新登录
        return "redirect:/login";
    }

    @GetMapping(value = {"logout"})
    public String logout(HttpSession session) {
        session.removeAttribute("user");
        return "redirect:/login";
    }
}
