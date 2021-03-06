package com.netease.yuqi.kmean;

import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Created with kmean.
 * User: hzyuqi1
 * Date: 2017/7/4
 * Time: 19:12
 * To change this template use File | Settings | File Templates.
 */

public class MapReduce {

    public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {

        //中心集合
        ArrayList<ArrayList<Double>> centers = null;
        //用k个中心
        int k = 0;

        //读取中心
        protected void setup(Context context) throws IOException,
                InterruptedException {
            centers = Utils.getCentersFromHDFS(context.getConfiguration().get("centersPath"),false);
            k = centers.size();
        }


        /**
         * 1.每次读取一条要分类的条记录与中心做对比，归类到对应的中心
         * 2.以中心ID为key，中心包含的记录为value输出(例如： 1 0.2 。  1为聚类中心的ID，0.2为靠近聚类中心的某个值)
         */
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //读取一行数据
            ArrayList<Double> fileds = Utils.textToArray(value);
            int sizeOfFileds = fileds.size();

            //double minDistance = 99999999;
            BigDecimal minDistance = new BigDecimal(9999999999999.0);
            int centerIndex = 0;

            //依次取出k个中心点与当前读取的记录做计算
            for(int i=0;i<k;i++){
                //double currentDistance = 0;
                BigDecimal currentDistance = new BigDecimal("0.0");
                for(int j=0;j<sizeOfFileds;j++){
                    //double centerPoint = Math.abs(centers.get(i).get(j));
                    //double filed = Math.abs(fileds.get(j));
                    //currentDistance += Math.pow((centerPoint - filed) / (centerPoint + filed), 2);

                    BigDecimal tmp1 = new BigDecimal(Math.abs(centers.get(i).get(j))) ;
                    BigDecimal tmp2 = new BigDecimal(Math.abs(fileds.get(j)));
                    BigDecimal tmp3 = tmp1.subtract(tmp2);
                    BigDecimal tmp4 = tmp1.add(tmp2);

                    if (tmp4.compareTo(BigDecimal.ZERO) == 0) {
                        continue;
                    }

                    currentDistance = currentDistance.add(new BigDecimal(tmp3.divide(tmp4,9, RoundingMode.HALF_DOWN).multiply(tmp3.divide(tmp4, 9, RoundingMode.HALF_DOWN)).toString()));
                }
                //循环找出距离该记录最接近的中心点的ID
                if(currentDistance.compareTo(minDistance) < 0){
                    minDistance = new BigDecimal(currentDistance.toString());
                    centerIndex = i;
                }
            }
            //以中心点为Key 将记录原样输出
            context.write(new IntWritable(centerIndex+1), value);
        }

    }

    //利用reduce的归并功能以中心为Key将记录归并到一起
    public static class Reduce extends Reducer<IntWritable, Text, Text, Text> {

        /**
         * 1.Key为聚类中心的ID value为该中心的记录集合
         * 2.计数所有记录元素的平均值，求出新的中心
         */
        protected void reduce(IntWritable key, Iterable<Text> value,Context context)
                throws IOException, InterruptedException {
            //ArrayList<ArrayList<Double>> filedsList = new ArrayList<ArrayList<Double>>();

            ArrayList<ArrayList<Double>> filedsList = Lists.newArrayList();
            //依次读取记录集，每行为一个ArrayList<Double>
            for(Iterator<Text> it = value.iterator(); it.hasNext();){
                ArrayList<Double> tempList = Utils.textToArray(it.next());
                filedsList.add(tempList);
            }

            //计算新的中心
            //每行的元素个数
            int filedSize = filedsList.get(0).size();

            BigDecimal[] avg = new BigDecimal[filedSize];
            //double[] avg = new double[filedSize];
            for(int i=0;i<filedSize;i++){
                //求没列的平均值
                //double sum = 0;
                BigDecimal sum = new BigDecimal("0.0");
                int size = filedsList.size();
                for(int j=0;j<size;j++){
                    sum = sum.add(new BigDecimal(String.valueOf(filedsList.get(j).get(i))));
                    //sum += filedsList.get(j).get(i);
                }
                //avg[i] = sum / size;
                avg[i] = new BigDecimal(sum.divide(new BigDecimal(String.valueOf(size)),9, RoundingMode.HALF_DOWN).toString());
                System.out.println("i = " + i + " avg[i] = " + avg[i].doubleValue());
            }
            double[] tmp = new double[filedSize];
            for (int i = 0; i < filedSize; i++) {
                tmp[i] = avg[i].doubleValue();
            }
            context.write(new Text("") , new Text(Arrays.toString(tmp).replace("[", "").replace("]", "")));
        }

    }

    @SuppressWarnings("deprecation")
    public static void run(String centerPath,String dataPath,String newCenterPath,boolean runReduce) throws IOException, ClassNotFoundException, InterruptedException{

        Configuration conf = new Configuration();
        conf.set("centersPath", centerPath);

        Job job = new Job(conf, "mykmeans");
        job.setJarByClass(MapReduce.class);

        job.setMapperClass(Map.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        //job.setNumReduceTasks(40);

        if(runReduce){
            //最后依次输出不许要reduce
            job.setReducerClass(Reduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
        }

        FileInputFormat.addInputPath(job, new Path(dataPath));

        FileOutputFormat.setOutputPath(job, new Path(newCenterPath));

        System.out.println(job.waitForCompletion(true));
    }

    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
//        String centerPath = "hdfs://hadoop702.lt.163.org:8020/user/data/centers.txt";
//        String dataPath = "hdfs://hadoop702.lt.163.org:8020/user/data/120Extrac";
//        String newCenterPath = "hdfs://hadoop702.lt.163.org:8020/user/data/output";

//
        String centerPath = "hdfs://hadoop702.lt.163.org:8020/user/big/centers.txt";
        String dataPath = "hdfs://hadoop702.lt.163.org:8020/user/big/DSIFTFeature";
        String newCenterPath = "hdfs://hadoop702.lt.163.org:8020/user/big/output";
        int count = 0;

        while(true){
            run(centerPath,dataPath,newCenterPath,true);
            System.out.println("the " + ++count + "th calculation");

            if(Utils.compareCenters(centerPath,newCenterPath) || count == 20){
                run(centerPath,dataPath,newCenterPath,false);
                break;
            }
        }
    }

}
