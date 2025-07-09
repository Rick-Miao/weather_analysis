package edu.jit.weather_analysis.controller;

import edu.jit.weather_analysis.entity.WeatherWritable;
import edu.jit.weather_analysis.repository.*;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 天气控制器
 *
 * @author 缪彭哲
 */
@Controller
public class WeatherController {
    @Resource
    private WeatherSearchRepository weatherSearchRepository;
    @Resource
    private CityCodeRepository cityCodeRepository;

    @GetMapping("/importData")
    public String importData() {
        // 从hdfs导入数据至hbase表weather中
        ImportDataRepository.run();
        return "index";
    }

    @GetMapping("/importAllData")
    public String importAllData() {
        // 从hdfs导入数据至hbase表weather中
        ImportAllDataRepository.run();
        return "index";
    }

    @GetMapping("/search")
    public String search(Model model) {
        model.addAttribute("cityCodeNameMap", cityCodeRepository.getAllCityCodesWithNames());
        return "search";
    }

    @PostMapping("/searchWeather")
    public String search(String code, String date, Model model) {
        // 调用查询方法
        WeatherWritable weather = weatherSearchRepository.search(code, date);
        // 必须重新传递城市数据
        model.addAttribute("cityCodeNameMap", cityCodeRepository.getAllCityCodesWithNames());
        model.addAttribute("weather", weather);

        // 保留用户选择的城市和日期值
        model.addAttribute("selectedCode", code);
        model.addAttribute("selectedDate", date);
        return "search";
    }

    @GetMapping("/summary")
    public String getSummary(Model model) {
        model.addAttribute("cityCodeNameMap", cityCodeRepository.getAllCityCodesWithNames());
        // 跳转到统计页面
        return "summary";
    }

    @PostMapping("/summaryWeather")
    public String Summary(String code, Model model) {
        // 调用统计每年的天气方法
        // SummaryRepository.summaryByCode();
        // SummaryRepository.fullSummary();
        // 调用方法返回所有每年的天气数据
        List<WeatherWritable> weathers = SummaryRepository.getSummaryByCode(code);
        // 传递给前端页面
        model.addAttribute("cityCodeNameMap", cityCodeRepository.getAllCityCodesWithNames());
        model.addAttribute("weathers", weathers);
        // 保留用户选择的城市和日期值
        model.addAttribute("selectedCode", code);
        // 跳转到统计页面
        return "summary";
    }

    @GetMapping("/weatherStationSummary")
    public String getWeatherStationSummary(Model model) {
        // 调用方法进行统计
        WeatherStationSummaryRepository.summary();
        // 调用方法返回所有站点的统计数据
        List<WeatherWritable> weathers = WeatherStationSummaryRepository.getSummaryAll();
        // 传递给前端页面
        model.addAttribute("weathers", weathers);
        // 跳转到统计页面
        return "weatherStationSummary";
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

    @GetMapping("/count")
    public String getCount(Model model) {
        List<CountItem> counts = dataService.getCityDataCounts();
        model.addAttribute("counts", counts);
        return "count";
    }
}
