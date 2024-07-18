package com.cs6650;

/**
 * @author Rebecca Zhang
 * Created on 2024-06-23
 */
public class LifeRide {

    Integer resortID;

    String seasonID;

    String dayID;

    Integer skierID;

    Integer time;

    Integer liftID;

    public LifeRide(Integer resortID, String seasonID, String dayID, Integer skierID, Integer time, Integer liftID) {
        this.resortID = resortID;
        this.seasonID = seasonID;
        this.dayID = dayID;
        this.skierID = skierID;
        this.time = time;
        this.liftID = liftID;
    }

    public Integer getResortID() {
        return resortID;
    }

    public String getSeasonID() {
        return seasonID;
    }

    public String getDayID() {
        return dayID;
    }

    public Integer getSkierID() {
        return skierID;
    }

    public Integer getTime() {
        return time;
    }

    public Integer getLiftID() {
        return liftID;
    }

    public void setResortID(Integer resortID) {
        this.resortID = resortID;
    }

    public void setSeasonID(String seasonID) {
        this.seasonID = seasonID;
    }

    public void setDayID(String dayID) {
        this.dayID = dayID;
    }

    public void setSkierID(Integer skierID) {
        this.skierID = skierID;
    }

    public void setTime(Integer time) {
        this.time = time;
    }

    public void setLiftID(Integer liftID) {
        this.liftID = liftID;
    }

}
