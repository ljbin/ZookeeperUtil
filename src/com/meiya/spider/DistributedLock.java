/**   
 * Copyright (c) 2012-2022 厦门市美亚柏科信息股份有限公司.
 */
package com.meiya.spider;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * @Description: 分布式共享锁实现
 * 
 * @Creator：linjb 2016-5-30
 */
public class DistributedLock {

    private static final byte[] data = { 0x12, 0x34 };
    private static final int DEFAULT_TIMEOUT_PERIOD = 30000;
    private ZooKeeper zookeeper = null;
    private final String zkHost; // ZK地址
    private final String root; // 根节点路径
    // 确保连接zk成功；
    private CountDownLatch connectedSemaphore = new CountDownLatch(1);
    private String id;
    private LockNode idName;
    private String ownerId;
    private String lastChildId;
    private Throwable other = null;
    private KeeperException exception = null;
    private InterruptedException interrupt = null;

    public DistributedLock(String zkHost, String root) {
        this.zkHost = zkHost;
        this.root = root;
        ensureExists(root);
    }

    /**
     * 尝试获取锁操作，阻塞式可被中断
     * 
     * @throws NestableRuntimeException
     */
    public void lock() throws InterruptedException, KeeperException, NestableRuntimeException {
        // 可能初始化的时候就失败了
        if (exception != null) {
            throw exception;
        }

        if (interrupt != null) {
            throw interrupt;
        }

        if (other != null) {
            throw new NestableRuntimeException(other);
        }

        if (isOwner()) {// 锁重入
            return;
        }

        BooleanMutex mutex = new BooleanMutex();
        acquireLock(mutex);
        // 避免zookeeper重启后导致watcher丢失，会出现死锁使用了超时进行重试
        try {
            mutex.get(DEFAULT_TIMEOUT_PERIOD, TimeUnit.MICROSECONDS);// 阻塞等待值为true
            // mutex.get();
        } catch (TimeoutException e) {
            if (!mutex.state()) {
                lock();
            }
        }

        if (exception != null) {
            throw exception;
        }

        if (interrupt != null) {
            throw interrupt;
        }

        if (other != null) {
            throw new NestableRuntimeException(other);
        }
    }

    /**
     * 尝试获取锁对象, 不会阻塞
     * 
     * @throws InterruptedException
     * @throws KeeperException
     * @throws NestableRuntimeException
     */
    public boolean tryLock() throws KeeperException, NestableRuntimeException {
        // 可能初始化的时候就失败了
        if (exception != null) {
            throw exception;
        }

        if (isOwner()) {// 锁重入
            return true;
        }

        acquireLock(null);

        if (exception != null) {
            throw exception;
        }

        if (interrupt != null) {
            Thread.currentThread().interrupt();
        }

        if (other != null) {
            throw new NestableRuntimeException(other);
        }

        return isOwner();
    }

    /**
     * 释放锁对象
     */
    public void unlock() throws KeeperException {
        if (id != null) {
            try {
                zookeeper.delete(root + "/" + id, -1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (KeeperException.NoNodeException e) {
                // do nothing
            } finally {
                id = null;
            }
        } else {
            // do nothing
        }
    }

    private void ensureExists(final String path) {
        try {
            zookeeper = new ZooKeeper(zkHost, DEFAULT_TIMEOUT_PERIOD, new Watcher() {

                @Override
                public void process(WatchedEvent event) {
                    if (Event.EventType.None == event.getType()) {
                        connectedSemaphore.countDown();
                    }

                }
            });
            connectedSemaphore.await();
            Stat stat = zookeeper.exists(path, false);
            if (stat != null) {
                return;
            }
            zookeeper.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            exception = e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            interrupt = e;
        } catch (IOException e) {
            other = e;
        }
    }

    /**
     * 关闭ZK连接
     */
    public void releaseConnection() {
        if (this.zookeeper != null) {
            try {
                this.zookeeper.close();
            } catch (InterruptedException e) {
            }
        }
    }

    /**
     * 返回锁对象对应的path
     */
    public String getRoot() {
        return root;
    }

    /**
     * 判断当前是不是锁的owner
     */
    public boolean isOwner() {
        return id != null && ownerId != null && id.equals(ownerId);
    }

    /**
     * 返回当前的节点id
     */
    public String getId() {
        return this.id;
    }

    // ===================== helper method =============================

    /**
     * 执行lock操作，允许传递watch变量控制是否需要阻塞lock操作
     */
    private Boolean acquireLock(final BooleanMutex mutex) {
        try {
            do {
                if (id == null) {// 构建当前lock的唯一标识
                    long sessionId = zookeeper.getSessionId();
                    String prefix = "x-" + sessionId + "-";
                    // 如果第一次，则创建一个节点
                    String path = zookeeper.create(root + "/" + prefix, data, Ids.OPEN_ACL_UNSAFE,
                            CreateMode.EPHEMERAL_SEQUENTIAL);
                    int index = path.lastIndexOf("/");
                    id = StringUtils.substring(path, index + 1);
                    idName = new LockNode(id);
                }

                if (id != null) {
                    List<String> names = zookeeper.getChildren(root, false);
                    if (names.isEmpty()) {
                        id = null;// 异常情况，重新创建一个
                    } else {
                        // 对节点进行排序
                        SortedSet<LockNode> sortedNames = new TreeSet<LockNode>();
                        for (String name : names) {
                            sortedNames.add(new LockNode(name));
                        }

                        if (sortedNames.contains(idName) == false) {
                            id = null;// 清空为null，重新创建一个
                            continue;
                        }

                        // 将第一个节点做为ownerId
                        ownerId = sortedNames.first().getName();
                        if (mutex != null && isOwner()) {
                            mutex.set(true);// 直接更新状态，返回
                            return true;
                        } else if (mutex == null) {
                            return isOwner();
                        }

                        SortedSet<LockNode> lessThanMe = sortedNames.headSet(idName);
                        if (!lessThanMe.isEmpty()) {
                            // 关注一下排队在自己之前的最近的一个节点
                            LockNode lastChildName = lessThanMe.last();
                            lastChildId = lastChildName.getName();
                            // 异步watcher处理
                            Stat stat = zookeeper.exists(root + "/" + lastChildId, new Watcher() {

                            	@Override
								public void process(WatchedEvent event) {
                            		// 监控删除操作
                            		if(event.getType() == EventType.NodeDeleted){
                            			acquireLock(mutex);
                            		}
								}
                            	
                            });

                            if (stat == null) {
                                acquireLock(mutex);// 如果节点不存在，需要自己重新触发一下，watcher不会被挂上去
                            }
                        } else {
                            if (isOwner()) {
                                mutex.set(true);
                            } else {
                                id = null;// 可能自己的节点已超时挂了，所以id和ownerId不相同
                            }
                        }
                    }
                }
            } while (id == null);
        } catch (KeeperException e) {
            exception = e;
            if (mutex != null) {
                mutex.set(true);
            }
        } catch (InterruptedException e) {
            interrupt = e;
            if (mutex != null) {
                mutex.set(true);
            }
        } catch (Throwable e) {
            other = e;
            if (mutex != null) {
                mutex.set(true);
            }
        }

        if (isOwner() && mutex != null) {
            mutex.set(true);
        }
        return Boolean.FALSE;
    }

    public static void main(String[] args) {
        ExecutorService exeucotr = Executors.newCachedThreadPool();
        final int count = 3;
        final CountDownLatch latch = new CountDownLatch(count);
        final DistributedLock[] nodes = new DistributedLock[count];
        for (int i = 0; i < count; i++) {
            final DistributedLock node = new DistributedLock("172.16.2.212:2181", "/shardLocks");
            nodes[i] = node;
            exeucotr.submit(new Runnable() {

                public void run() {
                    try {
                        Thread.sleep(1000);
                        node.lock(); // 获取锁
                        Thread.sleep(100 + RandomUtils.nextInt(100));
                        System.out.println("id: " + node.getId() + " is leader: " + node.isOwner());

                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (NestableRuntimeException e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            node.unlock();
                            node.releaseConnection();
                        } catch (KeeperException e) {
                            e.printStackTrace();
                        }
                        latch.countDown();
                    }

                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("do shutdown!");
        exeucotr.shutdown();
    }
}
