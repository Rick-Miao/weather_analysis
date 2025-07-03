package edu.jit.weather_analysis.entity;

import java.io.Serializable;

/**
 * 用户实体类
 *
 * @author 缪彭哲
 */
public class User implements Serializable {
    private static final long serialVersionUID = 1231771350141302767L;
    private String username;
    private String password;
    private String nickname;
    private String phone;
    private String email;

    public User() {
    }

    public User(String password, String username, String nickname, String phone, String email) {
        this.password = password;
        this.username = username;
        this.nickname = nickname;
        this.phone = phone;
        this.email = email;
    }

    @Override
    public String toString() {
        return "User{" +
                "username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", nickname='" + nickname + '\'' +
                ", phone='" + phone + '\'' +
                ", email='" + email + '\'' +
                '}';
    }

    public String getNickname() {
        return nickname;
    }

    public void setNickname(String nickname) {
        this.nickname = nickname;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }


}
