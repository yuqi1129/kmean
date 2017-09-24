package com.kerberous_test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Created with kmean.
 * User: hzyuqi1
 * Date: 2017/8/30
 * Time: 14:34
 * To change this template use File | Settings | File Templates.
 */

public class KerberousTest {

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFs", "hdfs://hz-cluster1");
        configuration.set("hadoop.security.authentication", "kerberos");
//        configuration.set("hadoop.security.authorization", "false");
        //configuration.addResource("hadoop-site-hz-cluster1.xml");
        configuration.set("fs.hdfs.impl", DistributedFileSystem.class.getCanonicalName());
        configuration.set("fs.file.impl", LocalFileSystem.class.getCanonicalName());

//        UserGroupInformation.setConfiguration(configuration);
        if (!AuthenticationUtil.authenticate("flume/inspur116.photo.163.org@HADOOP.HZ.NETEASE.COM", "flume.keytab", "flume")) {
            System.out.println("authenticate failed");
        }
        try {
//            UserGroupInformation.setConfiguration(configuration);
//            UserGroupInformation.loginUserFromKeytab("flume/inspur116.photo.163.org@HADOOP.HZ.NETEASE.COM", "flume.keytab");

            FileSystem fs = FileSystem.get(configuration);
            FileStatus[] statuses = fs.listStatus(new Path("/"));
            for (FileStatus fileStatus : statuses) {
                System.out.print(fileStatus.getPath().toString());
            }

            System.out.println(fs);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
