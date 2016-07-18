package utilities;

/**
 * @author Tung NGUYEN-DUY, <tungnd.ptit@gmail.com>
 * @date 20/06/2016
 */
public class TimeMeasurent {

    private long oldTime;

    public TimeMeasurent() {
        restart();
    }

    public long getDistance(){
        return getCurrentTime()-oldTime;
    }

    public long getDistanceAndRestart(){
        long dis = getDistance();
        System.out.println("Times since last restart(ms) : " + dis );
        restart();
        return dis;
    }

    public void restart(){
        oldTime = getCurrentTime();
        TimeUtils.printSystemCurrentTime();
    }

    public long getCurrentTime(){
        return System.currentTimeMillis();
    }
}
