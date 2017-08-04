package com.netease.yuqi.kmean;


import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with kmean.
 * User: hzyuqi1
 * Date: 2017/7/4
 * Time: 19:17
 * To change this template use File | Settings | File Templates.
 */


public class Utils {

    private static final BigDecimal BIG = new BigDecimal("0.0000000001");

    //读取中心文件的数据
    public static ArrayList<ArrayList<Double>> getCentersFromHDFS(String centersPath, boolean isDirectory) throws IOException {

        ArrayList<ArrayList<Double>> result = new ArrayList<ArrayList<Double>>();

        Path path = new Path(centersPath);

        Configuration conf = new Configuration();

        FileSystem fileSystem = path.getFileSystem(conf);

        if(isDirectory){
            FileStatus[] listFile = fileSystem.listStatus(path);
            for (int i = 0; i < listFile.length; i++) {
                result.addAll(getCentersFromHDFS(listFile[i].getPath().toString(),false));
            }
            return result;
        }

        FSDataInputStream fsis = fileSystem.open(path);
        LineReader lineReader = new LineReader(fsis, conf);

        Text line = new Text();

        while(lineReader.readLine(line) > 0){
            ArrayList<Double> tempList = textToArray(line);
            result.add(tempList);
        }
        lineReader.close();
        return result;
    }

    //删掉文件
    public static void deletePath(String pathStr) throws IOException{
        Configuration conf = new Configuration();
        Path path = new Path(pathStr);
        FileSystem hdfs = path.getFileSystem(conf);
        hdfs.delete(path ,true);
    }

    public static ArrayList<Double> textToArray(Text text){
        ArrayList<Double> list = new ArrayList<Double>();
        String string = text.toString().trim();
        if (string.endsWith(",")) {
            string = string.substring(0, string.length() - 1);
        }
        String fileds[] = string.split(",");

        for(int i=0;i<fileds.length;i++){
            list.add(Double.parseDouble(fileds[i]));
        }
        return list;
    }

    public static boolean compareCenters(String centerPath,String newPath) throws IOException{

        List<ArrayList<Double>> oldCenters = Utils.getCentersFromHDFS(centerPath,false);
        List<ArrayList<Double>> newCenters = Utils.getCentersFromHDFS(newPath,true);

        int size = oldCenters.size();
        int fildSize = oldCenters.get(0).size();
        int newSize = newCenters.size();
        System.out.println("oldSize = " + size + " newSize = " + newCenters.size());
        if (newSize != size) {
            for (int i = 0; i < size - newSize; i++) {
                ArrayList<Double> list = Lists.newArrayList();
                for (int j = 0; j < fildSize; j++) {
                    list.add(0.0);
                }
                newCenters.add(list);
            }
        }
        System.out.println("Now oldSize = " + size + " newSize = " + newCenters.size());




        //double distance = 0;
        BigDecimal distance = new BigDecimal("0.0");

        for(int i=0;i<size;i++){
            for(int j=0;j<fildSize;j++){

                BigDecimal bigDecimal1 = new BigDecimal(String.valueOf(oldCenters.get(i).get(j)));
                BigDecimal bigDecimal2 = new BigDecimal(String.valueOf(newCenters.get(i).get(j)));
                distance = distance.add(bigDecimal1.subtract(bigDecimal2).multiply(bigDecimal1.subtract(bigDecimal2)));

//                double t1 = Math.abs(oldCenters.get(i).get(j));
//                double t2 = Math.abs(newCenters.get(i).get(j));
//                distance += Math.pow((t1 - t2) / (t1 + t2), 2);
            }
        }
        System.out.println("distance = " + distance.toString());

        if(distance.abs().compareTo(BIG) < 0){
            //删掉新的中心文件以便最后依次归类输出
            Utils.deletePath(newPath);
            return true;
        }else{
            //先清空中心文件，将新的中心文件复制到中心文件中，再删掉中心文件

            Configuration conf = new Configuration();
            Path outPath = new Path(centerPath);
            FileSystem fileSystem = outPath.getFileSystem(conf);

            FSDataOutputStream overWrite = fileSystem.create(outPath,true);
            overWrite.writeChars("");
            overWrite.close();


            Path inPath = new Path(newPath);
            FileStatus[] listFiles = fileSystem.listStatus(inPath);
            for (int i = 0; i < listFiles.length; i++) {
                FSDataOutputStream out = fileSystem.create(outPath);
                FSDataInputStream in = fileSystem.open(listFiles[i].getPath());
                IOUtils.copyBytes(in, out, 4096, true);
            }
            //删掉新的中心文件以便第二次任务运行输出
            Utils.deletePath(newPath);
        }

        return false;
    }


    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        String centerPath = "hdfs://hadoop702.lt.163.org:8020/sloth-fs-checkpoints/meta/1_3";
        String dataPath = "hdfs://hadoop702.lt.163.org:8020/user/data/wine";
        String newCenterPath = "hdfs://hadoop702.lt.163.org:8020/user/data/output";

        int count = 0;

        URI uri = URI.create(centerPath);
        Configuration configuration = new Configuration();

        FileSystem dir = FileSystem.get(uri, configuration);
        FileStatus[] fileStatuses = dir.listStatus(new Path(centerPath));
        for (int i = 0; i < fileStatuses.length; i++) {
            System.out.println(fileStatuses[i].getPath().toString());
            /**
            FSDataInputStream in = dir.open(fileStatuses[i].getPath());
            ByteArrayOutputStream bo = new ByteArrayOutputStream();
            byte[] b = new byte[1];
            while (in.read(b) != -1) {
                bo.write(b);
            }

            String s = new String(bo.toByteArray());
            if (s.contains("0b37e282eb6e79b127eddeeb376927f6")) {
                System.out.println(s);
                break;
            }*/
        }
    }
    /**
    public static void main(String[] args) {
        Date date = new Date(1200000000);
        System.out.println(date.toString());
        System.out.println(date.getTime());

        Timestamp timestamp = new Timestamp(1050600000000L);
        System.out.println(timestamp.toString());
    }*/


    public static String LPAD(String s, int length, char c) {
        Preconditions.checkNotNull(s);
        Preconditions.checkArgument(length >= 0);

        int stringLen = s.length();
        if (length <= stringLen) {
            return s.substring(0, stringLen);
        } else {
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < length - stringLen; i++) {
                stringBuilder.append(c);
            }
            stringBuilder.append(s);
            return stringBuilder.toString();
        }
    }
}
