/**   
 * Copyright (c) 2012-2022 厦门市美亚柏科信息股份有限公司.
 */
package com.meiya.spider;

import java.text.MessageFormat;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.zookeeper.KeeperException;

/**
 * @Description: 分布式二层锁
 * @Creator：linjb 2016-5-30
 */
public class DistributedReentrantLock extends DistributedLock {

    private static final String ID_FORMAT = "Thread[{0}] Distributed[{1}]";
    private ReentrantLock reentrantLock = new ReentrantLock();

    public DistributedReentrantLock(String zkHost, String root) {
        super(zkHost, root);
    }

    public void lock() throws InterruptedException, KeeperException, NestableRuntimeException {
        reentrantLock.lock();// 多线程竞争时，先拿到第一层锁
        super.lock();
    }

    public boolean tryLock() throws KeeperException, NestableRuntimeException {
        // 多线程竞争时，先拿到第一层锁
        return reentrantLock.tryLock() && super.tryLock();
    }

    public void unlock() throws KeeperException {
        super.unlock();
        reentrantLock.unlock();// 多线程竞争时，释放最外层锁
    }

    @Override
    public String getId() {
        return MessageFormat.format(ID_FORMAT, Thread.currentThread().getId(), super.getId());
    }

    @Override
    public boolean isOwner() {
        return reentrantLock.isHeldByCurrentThread() && super.isOwner();
    }

    public static void main(String[] args) {
        ExecutorService exeucotr = Executors.newCachedThreadPool();
        final int count = 10;
        final DistributedReentrantLock lock = new DistributedReentrantLock("172.16.2.212:2181", "/shardLocks");  
        for (int i = 0; i < count; i++) {
            exeucotr.submit(new Runnable() {

                public void run() {
                    try {
                        Thread.sleep(1000);
                        lock.lock();
                        System.out.println("id: " + lock.getId() + " is leader: " + lock.isOwner());
                        Thread.sleep(100 + RandomUtils.nextInt(100));
                    } catch (InterruptedException e) {
                    } catch (KeeperException e) {
                    } catch (NestableRuntimeException e) {
                    } finally {
                        try {
                            lock.unlock();
                        } catch (KeeperException e) {
                        }
                    }

                }
            });
        }

        exeucotr.shutdown();
    }
}
