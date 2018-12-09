package com.zyy.util;

import java.io.*;

/**
 * Created by Administrator on 2018/10/25.
 */
public class ReadFile {

    public static void main(String[] args) {
        String sourceDir = "F:\\BaiduYunDownload";
//        String targetDir = "F:\\BaiduYunDownload\\笔记汇总";
        String targetDir = "C:\\Users\\Administrator.360XT-20160121I\\Desktop\\大数据\\文档";
        mv(new File(sourceDir), new File(targetDir));
    }

    public static void mv(File file, File targetDir) {
        if (file.isDirectory()) {

            File[] files = file.listFiles();
            for (File file1 : files) {
                mv(file1, targetDir);
            }

        } else if (file.isFile() && file.getName().contains("pdf")) {
            //mv
            System.out.println("--" + file.getAbsolutePath());
            mvFile(file, targetDir);

        }
    }

    public static void mvFile(File sourceFile, File targetDir) {

        InputStream inputStream = null;
        OutputStream outputStream = null;
        try {
            inputStream = new FileInputStream(sourceFile);
            byte[] data = new byte[1024];
            String[] parent = sourceFile.getParent().split("\\\\");
            outputStream = new FileOutputStream(targetDir + "/" + parent[parent.length - 1] + "-" + sourceFile.getName());
            int count = 0;
            while ((count = inputStream.read(data)) > 0) {
                outputStream.write(data, 0, count);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("mv " + sourceFile.getName() + " to " + targetDir.getAbsolutePath() + " success");
        }
    }


}
