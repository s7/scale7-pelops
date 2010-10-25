package org.scale7.cassandra.pelops;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

public class JmxMBeanManager {
    private static final Object mbsCreateMonitor = new Object();
    private static JmxMBeanManager thisObj;

    private MBeanServer mbs;

    public JmxMBeanManager() {
        mbs = ManagementFactory.getPlatformMBeanServer();
    }

    public static JmxMBeanManager getInstance() {
        synchronized (mbsCreateMonitor) {
            if (null == thisObj) {
                thisObj = new JmxMBeanManager();
            }
        }

        return thisObj;
    }

    public void registerMBean(Object theBean, String name) {
        try {
            ObjectName objName = new ObjectName(name);
            mbs.registerMBean(theBean, objName);
        }
        catch (Exception e) {
            throw new RuntimeException("exception while registering MBean, " + name, e);
        }
    }

    public Object getAttribute(String objName, String attrName) {
        try {
            ObjectName on = new ObjectName(objName);
            return mbs.getAttribute(on, attrName);
        }
        catch (Exception e) {
            throw new RuntimeException("exception while getting MBean attribute, " + objName + ", " + attrName, e);
        }
    }

    public Integer getIntAttribute(String objName, String attrName) {
        return (Integer) getAttribute(objName, attrName);
    }

    public String getStringAttribute(String objName, String attrName) {
        return (String) getAttribute(objName, attrName);
    }
}
