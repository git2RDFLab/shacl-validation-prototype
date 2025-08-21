package de.leipzig.htwk.gitrdf.shacl.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import de.leipzig.htwk.gitrdf.shacl.config.MemoryOptimizationConfig;

/**
 * Controller for memory management and monitoring
 */
@RestController
@RequestMapping("/api/memory")
public class MemoryController {

    private static final Logger logger = LoggerFactory.getLogger(MemoryController.class);

    @Autowired
    private MemoryOptimizationConfig memoryOptimizationConfig;

    /**
     * Get current memory statistics
     */
    @GetMapping("/stats")
    public ResponseEntity<MemoryStatsResponse> getMemoryStats() {
        logger.info("[STATS] Memory stats requested");
        
        var stats = memoryOptimizationConfig.getMemoryStats();
        
        MemoryStatsResponse response = new MemoryStatsResponse(
            stats.usedMb(),
            stats.totalMb(),
            stats.freeMb(),
            stats.maxMb(),
            stats.getUsagePercentage(),
            stats.getSummary(),
            memoryOptimizationConfig.isEnableAggressiveGc(),
            memoryOptimizationConfig.isEnableIsolatedValidation()
        );
        
        return ResponseEntity.ok(response);
    }

    /**
     * Force immediate garbage collection
     */
    @PostMapping("/gc")
    public ResponseEntity<GcResponse> forceGarbageCollection() {
        logger.info("[GC] Manual garbage collection requested");
        
        var beforeStats = memoryOptimizationConfig.getMemoryStats();
        long startTime = System.currentTimeMillis();
        
        memoryOptimizationConfig.forceGarbageCollection("manual-request");
        
        var afterStats = memoryOptimizationConfig.getMemoryStats();
        long duration = System.currentTimeMillis() - startTime;
        long freedMemory = beforeStats.usedMb() - afterStats.usedMb();
        
        GcResponse response = new GcResponse(
            true,
            duration,
            beforeStats.usedMb(),
            afterStats.usedMb(),
            freedMemory,
            String.format("Garbage collection completed in %dms. Freed %dMB memory (%dMB -> %dMB)", 
                duration, freedMemory, beforeStats.usedMb(), afterStats.usedMb())
        );
        
        logger.info("[SUCCESS] Manual garbage collection completed: {}", response.message());
        
        return ResponseEntity.ok(response);
    }

    /**
     * Get memory optimization configuration
     */
    @GetMapping("/config")
    public ResponseEntity<MemoryConfigResponse> getMemoryConfig() {
        MemoryConfigResponse response = new MemoryConfigResponse(
            memoryOptimizationConfig.isEnableAggressiveGc(),
            memoryOptimizationConfig.isEnableIsolatedValidation(),
            memoryOptimizationConfig.getGcIntervalMinutes(),
            memoryOptimizationConfig.getMemoryThresholdMb()
        );
        
        return ResponseEntity.ok(response);
    }

    // Response DTOs
    public record MemoryStatsResponse(
        long usedMb,
        long totalMb,
        long freeMb,
        long maxMb,
        double usagePercentage,
        String summary,
        boolean aggressiveGcEnabled,
        boolean isolatedValidationEnabled
    ) {}

    public record GcResponse(
        boolean success,
        long durationMs,
        long memoryBeforeMb,
        long memoryAfterMb,
        long freedMemoryMb,
        String message
    ) {}

    public record MemoryConfigResponse(
        boolean enableAggressiveGc,
        boolean enableIsolatedValidation,
        int gcIntervalMinutes,
        long memoryThresholdMb
    ) {}
}