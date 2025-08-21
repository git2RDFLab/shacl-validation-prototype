package de.leipzig.htwk.gitrdf.shacl.core;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to aggressively clear ThreadLocal variables.
 * This is necessary because RDF4J and other libraries may use ThreadLocal storage
 * that persists even after validation completes, causing memory leaks.
 */
public class ThreadLocalCleaner {
    
    private static final Logger logger = LoggerFactory.getLogger(ThreadLocalCleaner.class);
    
    /**
     * Aggressively clear all ThreadLocal variables for the current thread.
     * This uses reflection to access and clear the ThreadLocalMap.
     */
    public static void clearAllThreadLocals() {
        try {
            Thread currentThread = Thread.currentThread();
            
            // Clear the main ThreadLocalMap
            clearThreadLocalMap(currentThread, "threadLocals");
            
            // Clear the inheritable ThreadLocalMap
            clearThreadLocalMap(currentThread, "inheritableThreadLocals");
            
            logger.debug("[CLEANUP] Cleared ThreadLocal variables for thread: {}", currentThread.getName());
            
        } catch (Exception e) {
            logger.debug("Warning: Could not clear ThreadLocal variables: {}", e.getMessage());
        }
    }
    
    /**
     * Clear a specific ThreadLocalMap field from the current thread
     */
    private static void clearThreadLocalMap(Thread thread, String fieldName) {
        try {
            Field field = Thread.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            Object threadLocalMap = field.get(thread);
            
            if (threadLocalMap != null) {
                // Get the clear method from ThreadLocalMap
                Method clearMethod = threadLocalMap.getClass().getDeclaredMethod("clear");
                clearMethod.setAccessible(true);
                clearMethod.invoke(threadLocalMap);
                
                logger.debug("[CLEANUP] Cleared {} for thread: {}", fieldName, thread.getName());
            }
            
        } catch (Exception e) {
            logger.debug("Warning: Could not clear {}: {}", fieldName, e.getMessage());
        }
    }
    
    /**
     * Clear specific ThreadLocal instances if known
     */
    public static void clearSpecificThreadLocal(ThreadLocal<?> threadLocal) {
        try {
            if (threadLocal != null) {
                threadLocal.remove();
                logger.debug("[CLEANUP] Cleared specific ThreadLocal instance");
            }
        } catch (Exception e) {
            logger.debug("Warning: Could not clear specific ThreadLocal: {}", e.getMessage());
        }
    }
}