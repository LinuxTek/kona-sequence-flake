/*
 * Resource.java
 *
 * Created on 23.06.2008
 *
 * Copyright (c) 2008 Johann Burkard (<mailto:jb@eaio.com>) <http://eaio.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
 * Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */
package com.linuxtek.kona.sequence.flake.uuid;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Closes resources of all kinds.
 *
 * @author <a href="mailto:jb@eaio.com">Johann Burkard</a>
 * @author Florian Schwarz
 * @version $Id: Resource.java 4543 2012-01-27 17:35:55Z johann $
 * @see <a href="http://johannburkard.de/blog/programming/java/centralizing-resource-closing.html">Centralizing Resource
 * Closing for Cleaner Code, Fun and Profit</a>
 */
public final class Resource {

    private static final Logger LOG = LoggerFactory.getLogger(Resource.class);

    private static Map<String, String> insteadOfClose = new ConcurrentHashMap<String, String>(13);

    private static Map<String, String> beforeClose = new ConcurrentHashMap<String, String>(5);

    private static Map<String, String> afterClose = new ConcurrentHashMap<String, String>(5);

    static {
        beforeClose("javax.jms.Connection", "stop");
        beforeClose("javax.imageio.ImageWriter", "reset");
        beforeClose("javax.imageio.stream.ImageInputStream", "flush");
        
        insteadOfClose("com.eaio.nativecall.NativeCall", "destroy");
        insteadOfClose("com.jcraft.jsch.Channel", "disconnect");
        insteadOfClose("de.intarsys.cwt.environment.IGraphicsContext", "dispose");
        insteadOfClose("groovyx.net.http.HTTPBuilder", "shutdown");
        insteadOfClose("java.lang.Process", "destroy");
        insteadOfClose("javax.imageio.ImageReader", "dispose");
        insteadOfClose("javax.imageio.ImageWriter", "dispose");
        insteadOfClose("org.apache.http.impl.client.AbstractHttpClient", "shutdown");
        insteadOfClose("org.infinispan.Cache", "stop");
        insteadOfClose("org.infinispan.manager.DefaultCacheManager", "stop");
    }

    /**
     * No instances needed.
     */
    private Resource() {
        super();
    }

    /**
     * Call a certain <code>void</code> method before calling the <code>close</code> method.
     *
     * @param clazz the class, can be an interface, too, may not be <code>null</code>
     * @param method the method name, may not be <code>null</code>
     */
    public static void beforeClose(Class<?> clazz, String method) {
        beforeClose(clazz.getName(), method);
    }

    /**
     * Call a certain <code>void</code> method before calling the <code>close</code> method.
     *
     * @param className the class name, may not be <code>null</code>
     * @param method the method name, may not be <code>null</code>
     */
    public static void beforeClose(String className, String method) {
        beforeClose.put(className, method);
    }

    /**
     * Call a certain <code>void</code> method after calling the <code>close</code> method.
     *
     * @param clazz the class, can be an interface, too, may not be <code>null</code>
     * @param method the method name, may not be <code>null</code>
     */
    public static void afterClose(Class<?> clazz, String method) {
        afterClose(clazz.getName(), method);
    }

    /**
     * Call a certain <code>void</code> method before calling the <code>close</code> method.
     *
     * @param className the class name, may not be <code>null</code>
     * @param method the method name, may not be <code>null</code>
     */
    public static void afterClose(String className, String method) {
        afterClose.put(className, method);
    }

    /**
     * Call a certain <code>void</code> method instead of calling the <code>close</code> method.
     *
     * @param clazz the class name, may not be <code>null</code>
     * @param method the method name, may not be <code>null</code>
     */
    public static void insteadOfClose(Class<?> clazz, String method) {
        insteadOfClose(clazz.getName(), method);
    }

    /**
     * Call a certain <code>void</code> method instead of calling the <code>close</code> method.
     *
     * @param className the class name, may not be <code>null</code>
     * @param method the method name, may not be <code>null</code>
     */
    public static void insteadOfClose(String className, String method) {
        insteadOfClose.put(className, method);
    }

    /**
     * Closes objects in the order they are given.
     *
     * @param objects any number of objects, may be <code>null</code> or individual objects may be <code>null</code>
     */
    public static void close(Object... objects) {
        if (objects == null) {
            return;
        }
        for (Object object : objects) {
            if (object != null) {
                callFromMap(beforeClose, object);
                if (!callFromMap(insteadOfClose, object)) {
                    callVoidMethod(object, "close");
                }
                callFromMap(afterClose, object);
            }
        }
    }

    private static boolean callFromMap(Map<String, String> map, Object object) {
        return callFromMap(map, object, object.getClass());
    }

    private static boolean callFromMap(Map<String, String> map, Object object, Class<?> currClass) {
        String currentClassName = currClass.getName();

        // First check class itself
        String voidMethod = map == null ? null : map.get(currentClassName);
        if (voidMethod != null) {
            callVoidMethod(object, voidMethod);
            return true;
        }
        // Then check interfaces
        else if (callFromMapFromInterfaces(map, object, currClass)) {
            return true;
        }
        // Finally check base classes recursively
        else if (hasSuperclass(currClass)) {
            if (callFromMap(map, object, currClass.getSuperclass())) {
                return true;
            }
        }
        return false;
    }

    private static boolean callFromMapFromInterfaces(Map<String, String> map, Object object, Class<?> currClass) {
        boolean atLeastOneMethodCalled = false;
        for (Class<?> currentInterface : currClass.getInterfaces()) {
            if (callFromMap(map, object, currentInterface)) {
                atLeastOneMethodCalled = true;
            }
        }
        return atLeastOneMethodCalled;
    }

    private static boolean hasSuperclass(Class<?> clazz) {
        return clazz != null && clazz != Object.class && clazz.getSuperclass() != null;
    }

    private static void callVoidMethod(Object object, String method) {
        try {
            Method m = object.getClass().getMethod(method, new Class<?>[] {});
            m.invoke(object, new Object[] {});
        }
        catch (NoSuchMethodException ex) {
            log(object, ex);
        }
        catch (IllegalArgumentException ex) {
            log(object, ex);
        }
        catch (IllegalAccessException ex) {
            log(object, ex);
        }
        catch (InvocationTargetException ex) {
            log(object, ex.getCause());
        }
        catch (SecurityException ex) {
            log(object, ex);
        }
    }

    private static void log(Object object, Throwable throwable) {
        if (LOG.isTraceEnabled()) {
            LOG.warn(object.getClass().getName(), throwable);
        }
        else if (LOG.isDebugEnabled()) {
            LOG.warn(object.getClass().getName() + ": " + throwable.getClass().getName() + ": "
                    + throwable.getLocalizedMessage());
        }
    }

}
