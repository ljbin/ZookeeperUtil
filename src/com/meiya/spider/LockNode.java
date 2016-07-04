/**   
* Copyright (c) 2012-2022 厦门市美亚柏科信息股份有限公司.
*/
package com.meiya.spider;

import org.junit.Assert;

/**
 * @Description: 锁节点
 * @Creator：linjb 2016-5-30
 */
public class LockNode implements Comparable<LockNode>{
    private final String name;
    private String       prefix;
    private int          sequence = -1;

    public LockNode(String name){
        Assert.assertNotNull(name, "id cannot be null");
        this.name = name;
        this.prefix = name;
        int idx = name.lastIndexOf('-');
        if (idx >= 0) {
            this.prefix = name.substring(0, idx);
            try {
                this.sequence = Integer.parseInt(name.substring(idx + 1));
            } catch (Exception e) {
                // ignore
            }
        }
    }

    public int compareTo(LockNode that) {
        int s1 = this.sequence;
        int s2 = that.sequence;
        if (s1 == -1 && s2 == -1) {
            return this.name.compareTo(that.name);
        }

        if (s1 == -1) {
            return -1;
        } else if (s2 == -1) {
            return 1;
        } else {
            return s1 - s2;
        }
    }

    public String getName() {
        return name;
    }

    public int getSequence() {
        return sequence;
    }

    public String getPrefix() {
        return prefix;
    }

    public String toString() {
        return name.toString();
    }

    // ==================== hashcode & equals方法=======================

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        LockNode other = (LockNode) obj;
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equals(other.name)) {
            return false;
        }
        return true;
    }
}
