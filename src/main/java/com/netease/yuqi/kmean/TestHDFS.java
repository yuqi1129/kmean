package com.netease.yuqi.kmean;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * Created with kmean.
 * User: hzyuqi1
 * Date: 2017/9/24
 * Time: 14:47
 * To change this template use File | Settings | File Templates.
 */

public class TestHDFS {
    public static void main(String[] args) {
        final String mrResultPath = "hdfs://sloth2.hz.163.org:8020/test/output_01/part-r-00000";
        //final String mrResultPath = "hdfs://hz-cluster1:8020/test/output_01/part-r-00000";
        try {
            Configuration conf = new Configuration();

            conf.set("fs.hdfs.impl",
                    org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
            );
            conf.set("fs.file.impl",
                    org.apache.hadoop.fs.LocalFileSystem.class.getName()
            );
            // your hadoop home
            conf.set("hadoop.home.dir", "/usr/ndp/current/hdfs_client");
            // this conf is set as your hadoop cluster set, comment it or not
            conf.set("ipc.client.fallback-to-simple-auth-allowed", "true");

            Path path = new Path(mrResultPath);
            FileSystem hdfs = FileSystem.get(new URI(mrResultPath), conf);

            boolean isDirectory = hdfs.isDirectory(path);
            //boolean isDirectory = false;
            if (isDirectory) {
                FileStatus[] fileStatuses = hdfs.listStatus(path);
                for (FileStatus fileStatus : fileStatuses) {
                    // mean this is mr result
                    if (fileStatus.getPath().toString().contains("part-r")) {
                        FSDataInputStream fsDataInputStream = hdfs.open(fileStatus.getPath());
                        // deal your data
                        System.out.print(IOUtils.toString(fsDataInputStream));
                    }
                }
            } else {
                // is just a file
                FSDataInputStream fsDataInputStream = hdfs.open(path);
                System.out.print(IOUtils.toString(fsDataInputStream));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
