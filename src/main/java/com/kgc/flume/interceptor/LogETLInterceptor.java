package com.kgc.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class LogETLInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        // etl（ 不符合要求的数据返回 null ）
        byte[] body = event.getBody();
        String log = new String(body, Charset.forName("UTF-8"));

        // 校验启动日志和事件日志（使用工具类来校验）
        if (log.contains("start")){
            if (LogUtils.validateStart(log)){
                return event;
            }
        }else {
            if (LogUtils.validateEvent(log)){
                return event;
            }
        }

        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        ArrayList<Event> interceptors = new ArrayList<>();

        for (Event event : events) {
            Event intercept1 = intercept(event);

            if (intercept1 != null){
                interceptors.add(event);
            }
        }

        return interceptors;
    }

    @Override
    public void close() {

    }

    //通过静态内部类来实例化该拦截器
    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
