/*
 * The MIT License
 *
 * Copyright (c) 2011 Dominic Williams, Daniel Washusen and contributors.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

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

    public boolean isRegistered(String name) {
        try {
            ObjectName objName = new ObjectName(name);
            return mbs.isRegistered(objName);
        }
        catch (Exception e) {
            throw new RuntimeException("exception while checking if MBean is registered, " + name, e);
        }
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

    public void unregisterMBean(String name) {
        try {
            ObjectName objName = new ObjectName(name);
            mbs.unregisterMBean(objName);
        }
        catch (Exception e) {
            throw new RuntimeException("exception while unregistering MBean, " + name, e);
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
