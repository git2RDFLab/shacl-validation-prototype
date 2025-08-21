package de.leipzig.htwk.gitrdf.shacl.core;

import java.io.InputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import de.leipzig.htwk.gitrdf.shacl.service.ShaclDiagnosticService;

/**
 * Isolated validation engine that runs each validation in a separate thread
 * with aggressive cleanup to ensure memory is properly released.
 * 
 * This approach uses thread isolation to ensure that any thread-local storage
 * or cached resources are completely cleaned up when the thread terminates.
 */
@Component
public class IsolatedValidationEngine {

    private static final Logger logger = LoggerFactory.getLogger(IsolatedValidationEngine.class);
    
    @Autowired
    private ShaclValidationEngine validationEngine;
    
    // Thread pool for isolated validation - threads are short-lived and disposable
    private final ExecutorService isolatedExecutor = Executors.newCachedThreadPool(new ValidationThreadFactory());
    
    /**
     * Validate in complete isolation - each validation runs in a separate thread
     * that is terminated and garbage collected after completion
     */
    public boolean validateIsolated(String orderId, InputStream rdfData, boolean strictMode) throws Exception {
        logger.info("[ISOLATION] Starting isolated validation for order: {} (strict: {})", orderId, strictMode);
        
        ValidationTask task = new ValidationTask(orderId, rdfData, strictMode, "v2", false);
        Future<ValidationResult> future = isolatedExecutor.submit(task);
        
        try {
            ValidationResult result = future.get(300, TimeUnit.SECONDS); // 5 minute timeout
            
            if (result.exception != null) {
                throw result.exception;
            }
            
            logger.info("[SUCCESS] Isolated validation completed for order: {} - Valid: {}", orderId, result.isValid);
            return result.isValid;
            
        } catch (Exception e) {
            logger.error("[ERROR] Isolated validation failed for order: {}", orderId, e);
            future.cancel(true); // Force cancel the task
            throw e;
        } finally {
            // Force cleanup of any resources
            forceThreadCleanup();
        }
    }
    
    /**
     * Validate with diagnostics in complete isolation
     */
    public ShaclDiagnosticService.ValidationDiagnostics validateIsolatedWithDiagnostics(
            String orderId, InputStream rdfData, boolean strictMode) throws Exception {
        return validateIsolatedWithDiagnostics(orderId, rdfData, strictMode, "v2");
    }

    public ShaclDiagnosticService.ValidationDiagnostics validateIsolatedWithDiagnostics(
            String orderId, InputStream rdfData, boolean strictMode, String shapeVersion) throws Exception {
        
        logger.info("[ISOLATION] Starting isolated validation with diagnostics for order: {} (strict: {}, shape: {})", orderId, strictMode, shapeVersion);
        
        ValidationTask task = new ValidationTask(orderId, rdfData, strictMode, shapeVersion, true);
        Future<ValidationResult> future = isolatedExecutor.submit(task);
        
        try {
            ValidationResult result = future.get(300, TimeUnit.SECONDS); // 5 minute timeout
            
            if (result.exception != null) {
                throw result.exception;
            }
            
            logger.info("[SUCCESS] Isolated validation with diagnostics completed for order: {}", orderId);
            return result.diagnostics;
            
        } catch (Exception e) {
            logger.error("[ERROR] Isolated validation with diagnostics failed for order: {}", orderId, e);
            future.cancel(true); // Force cancel the task
            throw e;
        } finally {
            // Force cleanup of any resources
            forceThreadCleanup();
        }
    }
    
    /**
     * Force aggressive thread cleanup to ensure memory is released
     */
    private void forceThreadCleanup() {
        try {
            // Clear any thread-local variables
            ThreadLocalCleaner.clearAllThreadLocals();
            
            // Suggest garbage collection
            System.gc();
            Thread.sleep(25);
            System.gc();
            
            // Brief pause to allow GC to work
            Thread.sleep(50);
            
        } catch (Exception e) {
            logger.debug("Warning during thread cleanup: {}", e.getMessage());
        }
    }
    
    /**
     * Custom thread factory that creates daemon threads with specific naming
     */
    private static class ValidationThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "validation-isolated-" + threadNumber.getAndIncrement());
            t.setDaemon(true); // Daemon threads don't prevent JVM shutdown
            t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }
    
    /**
     * Validation task that runs in isolation
     */
    private class ValidationTask implements Callable<ValidationResult> {
        private final String orderId;
        private final InputStream rdfData;
        private final boolean strictMode;
        private final String shapeVersion;
        private final boolean withDiagnostics;
        
        public ValidationTask(String orderId, InputStream rdfData, boolean strictMode, String shapeVersion, boolean withDiagnostics) {
            this.orderId = orderId;
            this.rdfData = rdfData;
            this.strictMode = strictMode;
            this.shapeVersion = shapeVersion;
            this.withDiagnostics = withDiagnostics;
        }
        
        @Override
        public ValidationResult call() throws Exception {
            try {
                logger.debug("[THREAD] Validation task starting in thread: {}", Thread.currentThread().getName());
                
                if (withDiagnostics) {
                    ShaclDiagnosticService.ValidationDiagnostics diagnostics = 
                        validationEngine.validateWithDiagnostics(orderId, rdfData, strictMode, shapeVersion);
                    return new ValidationResult(diagnostics);
                } else {
                    boolean isValid = validationEngine.validate(orderId, rdfData, strictMode, shapeVersion);
                    return new ValidationResult(isValid);
                }
                
            } catch (Exception e) {
                logger.error("[FAILURE] Validation task failed in thread: {}", Thread.currentThread().getName(), e);
                return new ValidationResult(e);
            } finally {
                // Aggressive cleanup within the isolated thread
                try {
                    logger.debug("[CLEANUP] Cleaning up validation thread: {}", Thread.currentThread().getName());
                    
                    // Clear thread locals
                    ThreadLocalCleaner.clearAllThreadLocals();
                    
                    // Additional cleanup pause
                    Thread.sleep(25);
                    
                } catch (Exception e) {
                    logger.debug("Warning during thread-local cleanup: {}", e.getMessage());
                }
            }
        }
    }
    
    /**
     * Result wrapper for validation tasks
     */
    private static class ValidationResult {
        final boolean isValid;
        final ShaclDiagnosticService.ValidationDiagnostics diagnostics;
        final Exception exception;
        
        ValidationResult(boolean isValid) {
            this.isValid = isValid;
            this.diagnostics = null;
            this.exception = null;
        }
        
        ValidationResult(ShaclDiagnosticService.ValidationDiagnostics diagnostics) {
            this.isValid = diagnostics.isValid();
            this.diagnostics = diagnostics;
            this.exception = null;
        }
        
        ValidationResult(Exception exception) {
            this.isValid = false;
            this.diagnostics = null;
            this.exception = exception;
        }
    }
    
    /**
     * Shutdown the isolated executor when the application shuts down
     */
    public void shutdown() {
        logger.info("[SHUTDOWN] Shutting down isolated validation executor");
        isolatedExecutor.shutdown();
        try {
            if (!isolatedExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                isolatedExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            isolatedExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}