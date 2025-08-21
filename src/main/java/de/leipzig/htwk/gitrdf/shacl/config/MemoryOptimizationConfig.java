package de.leipzig.htwk.gitrdf.shacl.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import de.leipzig.htwk.gitrdf.shacl.core.IsolatedValidationEngine;
import jakarta.annotation.PreDestroy;

/**
 * Configuration for memory optimization and aggressive garbage collection
 */
@Configuration
@EnableScheduling
@ConfigurationProperties(prefix = "memory.optimization")
public class MemoryOptimizationConfig {
    
    private static final Logger logger = LoggerFactory.getLogger(MemoryOptimizationConfig.class);
    
    @Autowired(required = false)
    private IsolatedValidationEngine isolatedValidationEngine;
    
    // Configuration properties
    private boolean enableAggressiveGc = true;
    private boolean enableIsolatedValidation = true;
    private int gcIntervalMinutes = 5;
    private long memoryThresholdMb = 1024; // Trigger GC if memory usage > 1GB
    
    /**
     * Scheduled aggressive garbage collection for long-running validation services
     * This helps prevent memory accumulation from multiple validation runs
     */
    @Scheduled(fixedRateString = "#{${memory.optimization.gc-interval-minutes:5} * 60000}")
    public void performScheduledGarbageCollection() {
        if (!enableAggressiveGc) {
            return;
        }
        
        try {
            Runtime runtime = Runtime.getRuntime();
            long totalMemory = runtime.totalMemory();
            long freeMemory = runtime.freeMemory();
            long usedMemory = totalMemory - freeMemory;
            long usedMemoryMb = usedMemory / (1024 * 1024);
            
            logger.debug("[STATS] Memory usage: {}MB / {}MB", usedMemoryMb, totalMemory / (1024 * 1024));
            
            // Only trigger aggressive GC if memory usage is above threshold
            if (usedMemoryMb > memoryThresholdMb) {
                logger.info("[GC] Triggering scheduled aggressive garbage collection (usage: {}MB)", usedMemoryMb);
                
                // Multiple GC calls to ensure thorough cleanup
                System.gc();
                Thread.sleep(100);
                System.gc();
                Thread.sleep(50);
                System.gc();
                
                // Log memory after cleanup
                long newUsedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
                long freedMemory = usedMemoryMb - newUsedMemory;
                logger.info("[SUCCESS] Garbage collection completed. Freed: {}MB, Current usage: {}MB", 
                    freedMemory, newUsedMemory);
            }
            
        } catch (Exception e) {
            logger.warn("[WARNING] Error during scheduled garbage collection: {}", e.getMessage());
        }
    }
    
    /**
     * Force immediate garbage collection - can be called after large validations
     */
    public void forceGarbageCollection(String context) {
        if (!enableAggressiveGc) {
            return;
        }
        
        try {
            logger.info("[GC] Forcing immediate garbage collection for: {}", context);
            
            Runtime runtime = Runtime.getRuntime();
            long beforeMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
            
            // Aggressive GC sequence
            System.gc();
            Thread.sleep(50);
            System.gc();
            Thread.sleep(50);
            System.gc();
            Thread.sleep(50);
            System.gc();
            
            long afterMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
            long freedMemory = beforeMemory - afterMemory;
            
            logger.info("[SUCCESS] Forced GC completed for {}. Freed: {}MB ({}MB -> {}MB)", 
                context, freedMemory, beforeMemory, afterMemory);
                
        } catch (Exception e) {
            logger.warn("[WARNING] Error during forced garbage collection for {}: {}", context, e.getMessage());
        }
    }
    
    /**
     * Get current memory usage statistics
     */
    public MemoryStats getMemoryStats() {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        long maxMemory = runtime.maxMemory();
        
        return new MemoryStats(
            totalMemory / (1024 * 1024),
            usedMemory / (1024 * 1024),
            freeMemory / (1024 * 1024),
            maxMemory / (1024 * 1024)
        );
    }
    
    /**
     * Cleanup when application shuts down
     */
    @PreDestroy
    public void cleanup() {
        logger.info("[SHUTDOWN] Shutting down memory optimization components");
        
        if (isolatedValidationEngine != null) {
            isolatedValidationEngine.shutdown();
        }
        
        // Final cleanup
        forceGarbageCollection("application-shutdown");
    }
    
    // Getters and setters for configuration properties
    public boolean isEnableAggressiveGc() {
        return enableAggressiveGc;
    }
    
    public void setEnableAggressiveGc(boolean enableAggressiveGc) {
        this.enableAggressiveGc = enableAggressiveGc;
    }
    
    public boolean isEnableIsolatedValidation() {
        return enableIsolatedValidation;
    }
    
    public void setEnableIsolatedValidation(boolean enableIsolatedValidation) {
        this.enableIsolatedValidation = enableIsolatedValidation;
    }
    
    public int getGcIntervalMinutes() {
        return gcIntervalMinutes;
    }
    
    public void setGcIntervalMinutes(int gcIntervalMinutes) {
        this.gcIntervalMinutes = gcIntervalMinutes;
    }
    
    public long getMemoryThresholdMb() {
        return memoryThresholdMb;
    }
    
    public void setMemoryThresholdMb(long memoryThresholdMb) {
        this.memoryThresholdMb = memoryThresholdMb;
    }
    
    /**
     * Memory statistics record
     */
    public record MemoryStats(
        long totalMb,
        long usedMb,
        long freeMb,
        long maxMb
    ) {
        public double getUsagePercentage() {
            return (double) usedMb / totalMb * 100;
        }
        
        public String getSummary() {
            return String.format("Memory: %dMB used / %dMB total (%.1f%%), Max: %dMB", 
                usedMb, totalMb, getUsagePercentage(), maxMb);
        }
    }
}