package red.com.pwh.processing;

import red.com.pwh.dao.HourlyDAOInterface;

import java.time.LocalDate;
import java.time.LocalTime;

public interface ConditionsInterface {


    LocalTime get_morning_golden_hour(LocalTime sunrise);

    LocalTime get_evening_golden_hour(LocalTime sunset);

    LocalTime get_evening_blue_hour_start(LocalTime sunset);

    LocalTime get_morning_blue_hour_start(LocalTime sunrise);

    LocalTime get_evening_blue_hour_end(LocalTime sunset);

    LocalTime get_morning_blue_hour_end(LocalTime sunrise);

    LocalDate best_date_overall();


    LocalDate best_date_morning();

    LocalDate best_date_evening();

    LocalDate best_date_early_night();

    LocalDate best_date_late_night();
}
