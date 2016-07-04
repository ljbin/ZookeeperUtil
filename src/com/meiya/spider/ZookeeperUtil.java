/**   
 * Copyright (c) 2012-2022 厦门市美亚柏科信息股份有限公司.
 */
package com.meiya.spider;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * @Description: Zookeeper工具类
 * @Creator：linjb 2015-11-2
 */
public class ZookeeperUtil {

    private static final int SESSION_TIMEOUT = 30000;

    private ZooKeeper zookeeper;

    private static String host;

    private static String command;

    private static String path;

    private static String filePath;

    private Watcher watcher = new Watcher() {
        public void process(WatchedEvent event) {
            // ignore
        }
    };

    /**
     * @Description: 打开连接
     * @throws IOException
     * 
     * @History 1. 2015-11-2 linjb 创建方法
     */
    public void connect() throws IOException {
        zookeeper = new ZooKeeper(host, SESSION_TIMEOUT, watcher);
    }

    /**
     * @Description: 关闭连接
     * @throws IOException
     * 
     * @History 1. 2015-11-2 linjb 创建方法
     */
    public void close() throws IOException {
        if (zookeeper != null) {
            try {
                zookeeper.close();
            } catch (InterruptedException e) {
                System.out.println("zookeeper客户端关闭失败！");
            }
        }
    }

    public void createNode(String path, byte[] data) throws Exception {
        zookeeper.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public void setData(String path, String data) throws Exception {
        byte[] bytes = data.getBytes();
        zookeeper.setData(path, bytes, -1);
    }

    public void setData(String path, byte[] data) throws Exception {
        zookeeper.setData(path, data, -1);
    }

    /**
     * @Description: 根据路径获取数据
     * @param path
     * @return
     * @throws Exception
     * 
     * @History 1. 2015-11-2 linjb 创建方法
     */
    public String getDataStr(String path) throws Exception {
        byte[] bytes = zookeeper.getData(path, null, null);
        return new String(bytes);
    }

    /**
     * @Description: 根据路径获取数据
     * @param path
     * @return
     * @throws Exception
     * 
     * @History 1. 2015-11-2 linjb 创建方法
     */
    public byte[] getData(String path) throws Exception {
        return zookeeper.getData(path, null, null);
    }

    /**
     * @Description: main
     * @param args
     * @throws Exception
     * @History 1. 2016-3-22 linjb 创建方法
     */
    public static void main(String[] args) throws Exception {
//        command = args[0];
//        host = args[1];
//        path = args[2];
//        filePath = args[3];
         command = "update";
         host = "172.16.2.212:2181";
         path = "/clusterstate.json";
         filePath = "clusterstate.json";
        File file = new File(ZookeeperUtil.class.getResource("/").getPath(), filePath);
        ZookeeperUtil zk = new ZookeeperUtil();
        zk.connect();
        if ("get".equals(command)) {
            byte[] bytes = zk.getData(path);
            FileOutputStream out = new FileOutputStream(file);
            out.write(bytes, 0, bytes.length);
            out.close();
            System.out.println("获取" + path + "数据成功");
        } else if ("update".equals(command)) {
            Long length = file.length();
            byte[] data = new byte[length.intValue()];
            FileInputStream in = new FileInputStream(file);
            in.read(data);
            in.close();
            zk.setData(path, data);
            System.out.println("更新" + path + "数据成功");
        } else {
            System.out.println("输入命令错误！");
        }
        zk.close();
    }

}
