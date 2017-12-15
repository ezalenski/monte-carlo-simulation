package com.hortonworks.example;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ThisDate {
    int month;
    int day;
    int year;

    final int MIN_YEAR = 1990;

    public ThisDate(String date){
        String [] d = date.split("-");
        if(d.length == 3) {
            this.year = Integer.parseInt(d[0]);
            this.month = Integer.parseInt(d[1]);
            this.day = Integer.parseInt(d[2]);
        }
        else{
            this.year = -1;
            this.month = -1;
            this.day = -1;
        }
    }

    boolean checkValid(){
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date();
        ThisDate today = new ThisDate(dateFormat.format(date).toString());

        if(this.year < MIN_YEAR || this.year > today.year){
            return false;
        }
        if(this.month > 12 || this.month < 1){
            return false;
        }
        if(this.month == 1 || this.month == 3 || this.month == 5 || this.month == 7 || this.month == 8 ||
                this.month == 10 || this.month == 12){
            if(this.day > 31){
                return false;
            }
        }
        else{
            if(this.month == 2){
                if(this.year % 4 == 0){
                    if(this.day > 29) {
                        return false;
                    }
                }
                else{
                    if(this.day > 28) {
                        return false;
                    }
                }
            }
            else{
                if(this.day > 30){
                    return false;
                }
            }
        }

        if(this.year == today.year){
            if(this.month > today.month){
                return false;
            }
            if(this.month == today.month){
                if(this.day > today.day){
                    return false;
                }
            }
        }

        return true;
    }

    public boolean isAfter(ThisDate prevDate){
        if(!this.checkValid()){
            return false;
        }
        if(this.year < prevDate.year){
            return false;
        }
        if(this.year == prevDate.year){
            if(this.month < prevDate.month){
                return false;
            }
            if(this.month == prevDate.month){
                if(this.day < prevDate.day){
                    return false;
                }
            }
        }
        return true;
    }
}
