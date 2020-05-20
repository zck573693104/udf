package com.situation;

import com.situation.utils.DateUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Date;

public class DateToTimeStamp extends ScalarFunction {

    public String DATE_FORMAT;

    public DateToTimeStamp() {
        this.DATE_FORMAT = "yyyy-MM-dd";
    }

    public DateToTimeStamp(String model) {
        this.DATE_FORMAT = model;
    }

    public String eval(String dates) {
        if (StringUtils.isBlank(dates)){
            return null;
        }
        try {
            Date date = DateUtil.strToDate(dates,DATE_FORMAT);
            return String.valueOf(date.getTime());
        } catch (Exception e) {
            return null;
        }
    }
}
