package de.leipzig.htwk.gitrdf.shacl.service;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import de.leipzig.htwk.gitrdf.database.common.entity.GithubRepositoryOrderEntity;
import de.leipzig.htwk.gitrdf.database.common.repository.GithubRepositoryOrderRepository;
import de.leipzig.htwk.gitrdf.shacl.core.ShaclValidationEngine;
import de.leipzig.htwk.gitrdf.shacl.model.OrderInfoData;
import de.leipzig.htwk.gitrdf.shacl.model.RdfDataResult;
import de.leipzig.htwk.gitrdf.shacl.model.ValidationResult;

@Service
public class ShaclValidationService {

  private static final Logger logger = LoggerFactory.getLogger(ShaclValidationService.class);

  @Autowired
  private ShaclValidationEngine validationEngine;
  @Autowired
  private de.leipzig.htwk.gitrdf.shacl.core.IsolatedValidationEngine isolatedValidationEngine;
  @Autowired
  private de.leipzig.htwk.gitrdf.shacl.config.MemoryOptimizationConfig memoryOptimizationConfig;
  @Autowired
  private RemoteRepositoryService remoteRepositoryService;
  @Autowired
  private GithubRepositoryOrderRepository githubOrderRepository;

  // ========== GITHUB VALIDATION METHODS ==========

  /**
   * Synchronous GitHub validation with memory optimization
   */
  public boolean validateGithubOrder(String orderId, boolean strictMode) throws Exception {
    RdfDataResult rdfData = remoteRepositoryService.fetchGithubRdf(orderId);
    
    // Log memory before validation
    var memoryBefore = memoryOptimizationConfig.getMemoryStats();
    logger.info("[STATS] Memory before validation {}: {}", orderId, memoryBefore.getSummary());
    
    // Ensure the original RDF data stream is properly closed after validation
    try {
      boolean result;
      
      // Use isolated validation if enabled for better memory management
      if (memoryOptimizationConfig.isEnableIsolatedValidation()) {
        logger.info("[ISOLATION] Using isolated validation for order: {}", orderId);
        result = isolatedValidationEngine.validateIsolated(orderId, rdfData.inputStream, strictMode);
      } else {
        logger.info("[STANDARD] Using standard validation for order: {}", orderId);
        result = validationEngine.validate(orderId, rdfData.inputStream, strictMode);
      }
      
      return result;
      
    } finally {
      // Critical: Close the original RDF data stream to prevent memory leaks
      if (rdfData.inputStream != null) {
        try {
          rdfData.inputStream.close();
          logger.debug("🧹 Closed original RDF data stream for order: {}", orderId);
        } catch (Exception e) {
          logger.warn("⚠️ Could not close original RDF data stream for order {}: {}", orderId, e.getMessage());
        }
      }
      
      // Force garbage collection after large validations
      memoryOptimizationConfig.forceGarbageCollection("github-order-" + orderId);
      
      // Log memory after validation and cleanup
      var memoryAfter = memoryOptimizationConfig.getMemoryStats();
      logger.info("[STATS] Memory after validation {}: {}", orderId, memoryAfter.getSummary());
      logger.info("[MEMORY] Memory freed for order {}: {}MB", orderId, 
          memoryBefore.usedMb() - memoryAfter.usedMb());
    }
  }

  public boolean validateGithubOrder(String orderId) throws Exception {
    return validateGithubOrder(orderId, false);
  }

  /**
   * Synchronous GitHub validation with detailed diagnostics and memory optimization
   */
  public ValidationResult validateGithubOrderWithDiagnostics(String orderId, boolean strictMode) throws Exception {
    return validateGithubOrderWithDiagnostics(orderId, strictMode, "v2");
  }

  public ValidationResult validateGithubOrderWithDiagnostics(String orderId, boolean strictMode, String shapeVersion) throws Exception {
    RdfDataResult rdfData = remoteRepositoryService.fetchGithubRdf(orderId);
    
    // Log memory before validation
    var memoryBefore = memoryOptimizationConfig.getMemoryStats();
    logger.info("[STATS] Memory before diagnostics validation {}: {}", orderId, memoryBefore.getSummary());
    
    // Ensure the original RDF data stream is properly closed after validation
    try {
      ShaclDiagnosticService.ValidationDiagnostics diagnostics;
      
      // Use isolated validation if enabled for better memory management
      if (memoryOptimizationConfig.isEnableIsolatedValidation()) {
        logger.info("[ISOLATION] Using isolated validation with diagnostics for order: {}", orderId);
        diagnostics = isolatedValidationEngine.validateIsolatedWithDiagnostics(orderId, rdfData.inputStream, strictMode, shapeVersion);
      } else {
        logger.info("[STANDARD] Using standard validation with diagnostics for order: {}", orderId);
        diagnostics = validationEngine.validateWithDiagnostics(orderId, rdfData.inputStream, strictMode, shapeVersion);
      }
      
      return new ValidationResult(
          orderId,
          "GitHub", 
          diagnostics.isValid(),
          diagnostics.durationMs(),
          strictMode,
          diagnostics.errorMessage(),
          diagnostics.violations(),
          diagnostics.validationReport()
      );
      
    } finally {
      // Critical: Close the original RDF data stream to prevent memory leaks
      if (rdfData.inputStream != null) {
        try {
          rdfData.inputStream.close();
          logger.debug("🧹 Closed original RDF data stream for order: {}", orderId);
        } catch (Exception e) {
          logger.warn("⚠️ Could not close original RDF data stream for order {}: {}", orderId, e.getMessage());
        }
      }
      
      // Force garbage collection after large validations
      memoryOptimizationConfig.forceGarbageCollection("github-diagnostics-" + orderId);
      
      // Log memory after validation and cleanup
      var memoryAfter = memoryOptimizationConfig.getMemoryStats();
      logger.info("[STATS] Memory after diagnostics validation {}: {}", orderId, memoryAfter.getSummary());
      logger.info("[MEMORY] Memory freed for diagnostics order {}: {}MB", orderId, 
          memoryBefore.usedMb() - memoryAfter.usedMb());
    }
  }



  /**
   * Get RDF data stream for GitHub order (for enhanced validation)
   */
  public java.io.InputStream getGithubOrderRdfStream(String orderId) throws Exception {
    logger.debug("[STREAM] Getting RDF stream for GitHub order: {}", orderId);
    RdfDataResult rdfData = remoteRepositoryService.fetchGithubRdf(orderId);
    return rdfData.inputStream;
  }

  /**
   * Get GitHub order information
   */
  @Transactional(readOnly = true)
  public OrderInfoData getGithubOrderInfo(Long orderId) {
    logger.debug("[STATS] Fetching GitHub order info for: {}", orderId);

    Optional<GithubRepositoryOrderEntity> orderOpt = githubOrderRepository.findById(orderId);

    if (orderOpt.isEmpty()) {
      logger.warn("[ERROR] GitHub order not found: {}", orderId);
      return null;
    }

    GithubRepositoryOrderEntity order = orderOpt.get();

    // Check if RDF is available via HTTP
    boolean hasRdfData = remoteRepositoryService.isGithubRdfAvailable(orderId.toString());

    logger.debug("[STATS] GitHub order {} - RDF available: {}", orderId, hasRdfData);

    return new OrderInfoData(orderId, "GitHub", order.getOwnerName(), order.getRepositoryName(),
        order.getStatus().toString(), hasRdfData);
  }



}