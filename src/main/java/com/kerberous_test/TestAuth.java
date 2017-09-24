package com.kerberous_test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Created with kmean.
 * User: hzyuqi1
 * Date: 2017/8/30
 * Time: 15:52
 * To change this template use File | Settings | File Templates.
 */

public class TestAuth {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hz-cluster3");
        conf.set("hadoop.security.authentication", "kerberos");

        UserGroupInformation.setConfiguration(conf);
        if (!AuthenticationUtil.authenticate("flume/inspur116.photo.163.org@HADOOP.HZ.NETEASE.COM", "flume.keytab", "flume")) {
            System.out.println("authenticate failed");
        } else {
            System.out.println("Success!");
        }
    }
}
