package utilities;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @author Tung NGUYEN-DUY, <tungnd.ptit@gmail.com>
 * @date 17/06/2016
 */
public class TimeUtils {

    public static final String FFDATE_TIME_1 = "yyyy-MM-dd HH:mm:ss";
    public static long stringToMilices(String time, String format){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        try {
            Date date = simpleDateFormat.parse(time);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            return cal.getTimeInMillis();
        } catch (ParseException ex) {
            System.out.println("Exception "+ex);
        }
        return 0;
    }
    public static long stringToMilices(String time){
        return stringToMilices(time, FFDATE_TIME_1);
    }


    public static long stringToSecond(String time, String format) {
        return stringToMilices(time, format) / 1000;
    }
    public static long stringToSecond(String time) {
        return stringToMilices(time) / 1000;
    }


    public static long stringToMinute(String time, String format){
        return stringToMilices(time, format) / 1000 / 60;
    }
    public static long stringToMinute(String time){
        return stringToMilices(time) / 1000 / 60;
    }

    public static void printSystemCurrentTime(){
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(FFDATE_TIME_1);
        System.out.println("Time at the moment : " + simpleDateFormat.format(cal.getTime()));
    }
}
