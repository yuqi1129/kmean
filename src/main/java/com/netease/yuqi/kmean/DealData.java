package com.netease.yuqi.kmean;

import com.google.common.collect.Lists;

import org.apache.commons.io.IOUtils;
import org.mortbay.util.IO;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Created with kmean.
 * User: hzyuqi1
 * Date: 2017/7/19
 * Time: 9:30
 * To change this template use File | Settings | File Templates.
 */

public class DealData {
    public static final int FILE_NUMBER = 120;
    public static final int CENTERS = 28;

    public static void main(String[] args) {
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        InputStream in = classLoader.getResourceAsStream("120result/result.txt");
        List<List<Integer>> resultList = Lists.newArrayList();
        List<List<String>> originData = Lists.newArrayList();

        for (int i = 0; i < FILE_NUMBER; i++) {
            List<Integer> element = Lists.newArrayList();
            for (int j = 0; j < CENTERS ; j++ ) {
                element.add(0);
            }
            resultList.add(element);
        }

        try {
            for (int i = 0; i < FILE_NUMBER; i++) {
                String fileName = "120Extrac/picture[" + String.valueOf(i) + "].txt";
                InputStream inputStream = classLoader.getResourceAsStream(fileName);
                List<String> lines = IOUtils.readLines(inputStream);
                originData.add(lines);
            }

            List<String> result = IOUtils.readLines(in);
            for (String line : result) {
                String[] centerAndData = line.split("\t");
                Integer center = Integer.valueOf(centerAndData[0].trim());
                String   s = centerAndData[1];

                for (int i = 0; i < originData.size(); i++) {
                    List<String> strings = originData.get(i);
                    for (int j = 0; j < strings.size(); j++) {
                        if (s.equals(strings.get(j))) {
                            resultList.get(i).set(center - 1, 1 + resultList.get(i).get(center -1));
                            break;
                        }
                    }
                }

            }

            OutputStream outputStream = new FileOutputStream("final.txt");
            for (List<Integer> list : resultList) {
                IOUtils.write(list.toString().replace("[","").replace("]","") + "\r\n", outputStream);
            }

            outputStream.close();
        }catch (IOException e) {
            e.printStackTrace();
        }

    }
}
