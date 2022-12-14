package red.com.pwh.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import red.com.pwh.entity.DailyWeather;
import red.com.pwh.service.PHServiceInterface;

import java.io.IOException;
import java.time.LocalDate;
import java.util.List;

@Controller
public class PWHController {

    private PHServiceInterface phServiceInterface;

    @Autowired
    public void setPhServiceInterface(PHServiceInterface phServiceInterface) {
        this.phServiceInterface = phServiceInterface;
    }

    @GetMapping("/")
    public String home(Model model){
        model.addAttribute("location",new String());
        return "home";
    }


    @PostMapping("/location")
    public String set_location(@RequestParam String location) throws IOException {
        phServiceInterface.set_location(location);
        return "redirect:/weather" ;
    }

    @GetMapping("/weather")
    public String show_weather(Model model) throws IOException {
        model.addAttribute("weather",phServiceInterface.get_day_weather());
        model.addAttribute("address",phServiceInterface.get_address());
        model.addAttribute("code",phServiceInterface.get_weather_code());
        model.addAttribute("best_day",phServiceInterface.get_bestDay_optional("").getDayOfWeek().toString());
        model.addAttribute("best_morning",phServiceInterface.get_bestDay_optional("morning").getDayOfWeek().toString());
        model.addAttribute("best_evening",phServiceInterface.get_bestDay_optional("evening").getDayOfWeek().toString());
        model.addAttribute("best_e_night",phServiceInterface.get_bestDay_optional("e_night").getDayOfWeek().toString());
        model.addAttribute("best_l_night",phServiceInterface.get_bestDay_optional("l_night").getDayOfWeek().toString());
        return "main";
    }

    @GetMapping("/daily")
    public String show_daily(@RequestParam("day") String day, Model model){
        LocalDate date = phServiceInterface.reverse_date(day);
        model.addAttribute("weather",phServiceInterface.get_hour_weather(date));
        model.addAttribute("golden",phServiceInterface.get_goldenHours(date));
        model.addAttribute("blue",phServiceInterface.get_blueHours(date));
        model.addAttribute("day",day);
        model.addAttribute("code",phServiceInterface.get_weather_code_hour(date));
        model.addAttribute("period",phServiceInterface.is_night(date));
        model.addAttribute("set_rise",phServiceInterface.get_sunset_sunrise(date));
        return "day_page";
    }
}
