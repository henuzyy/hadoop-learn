package com.zyy.util;

import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.httpclient.params.HttpMethodParams;

import javax.net.ssl.SSLSocketFactory;
import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2018/12/8.
 */
public class HttpClientUtil {

    private final static HttpClient HTTP_CLIENT = new HttpClient();

    static {
        //多线程且线程安全
        HTTP_CLIENT.setHttpConnectionManager(new MultiThreadedHttpConnectionManager());
        //设置等待数据超时时间
        HTTP_CLIENT.getHttpConnectionManager().getParams().setSoTimeout(5000);
        //连接超时
        HTTP_CLIENT.getHttpConnectionManager().getParams().setConnectionTimeout(10000);
        //设置从HTTPConnection连接池中获取HttpConnection的超时时间
        HTTP_CLIENT.getParams().setConnectionManagerTimeout(5000);

    }

    public static byte[] doPost(String url, Map<String, Object> params) {

        PostMethod method = new PostMethod(url);

        for (Map.Entry<String, Object> entry : params.entrySet()) {
            method.addParameter(new NameValuePair(entry.getKey(), entry.getValue().toString()));
        }

        try {
            int resultStatuCode = HTTP_CLIENT.executeMethod(method);
            if (resultStatuCode != HttpStatus.SC_OK) {
                System.out.println("Error statu:" + resultStatuCode);

            }
            return method.getResponseBody();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            method.releaseConnection();
        }
        return null;
    }

    public static byte[] doGet(String url, Map<String, Object> params) {
        StringBuilder paramsStr = new StringBuilder();
        for (Map.Entry<String, Object> map : params.entrySet()) {
            paramsStr.append(map.getKey()).append("=").append(map.getValue().toString()).append("&");
        }

        if (paramsStr.length() > 0) {
            url += "?" + paramsStr.toString().substring(0, paramsStr.toString().length() - 1);

        }

        System.out.println(url);
        return doGet(url);
    }

    public static byte[] doGet(String url) {


        GetMethod method = new GetMethod(url);

        method.setRequestHeader(HttpClientParams.USER_AGENT, "em-choice");
        method.setRequestHeader(HttpClientParams.HTTP_CONTENT_CHARSET, "text/plain;charset=UTF-8");
//        method.setRequestHeader(HttpClientParams.PROTOCOL_VERSION, HttpVersion.HTTP_1_1.toString());


        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
                new DefaultHttpMethodRetryHandler(3, false));

        try {

            int statusCode = HTTP_CLIENT.executeMethod(method);
            System.out.println("------------------------------" + System.currentTimeMillis());
            if (statusCode != HttpStatus.SC_OK) {
                System.err.println("Method failed: " + method.getStatusLine());
            }

            byte[] responseBody = method.getResponseBody();

            System.out.println(url + ": size:" + responseBody.length);
//            System.out.println(new String(responseBody));

            return responseBody;
        } catch (HttpException e) {
            e.printStackTrace();
            return new byte[0];
        } catch (IOException e) {
            e.printStackTrace();
            return new byte[0];
        } finally {
            // Release the connection.
            method.releaseConnection();
        }
    }


    public static byte[] doSSLGET(String url, Map<String, Object> params) {
        return new byte[0];
    }


    public static void main(String[] args) {
        Map<String, Object> map = new HashMap<>();
        map.put("1", "asdfasdfasdfgsdfg");
        map.put("2", "dldoqwepsdgg");
        String url = "http://18.com.cn/";

        doGet(url, map);

//        Thread thread1 = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                while (true) {
//                    HttpClientUtil.doGet("http://www.cnblogs.com");
//                }
//
//            }
//        });
//        thread1.setDaemon(false);
//        thread1.start();
//
//        Thread thread2 = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                while (true) {
//                    HttpClientUtil.doGet("http://www.18.com.cn");
//                }
//
//            }
//        });
//        thread2.setDaemon(false);
//        thread2.start();

    }
}
