package de.leipzig.htwk.gitrdf.shacl.core;

import static org.eclipse.rdf4j.model.vocabulary.RDF4J.SHACL_SHAPE_GRAPH;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.vocabulary.SHACL;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sail.shacl.ShaclSail;
import org.eclipse.rdf4j.sail.shacl.ShaclSailValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import de.leipzig.htwk.gitrdf.shacl.config.ValidationConfig;
import de.leipzig.htwk.gitrdf.shacl.service.ShaclDiagnosticService;
import de.leipzig.htwk.gitrdf.shacl.service.UriValidationService;

@Component
public class ShaclValidationEngine {

  private static final Logger logger = LoggerFactory.getLogger(ShaclValidationEngine.class);

  @Autowired
  private UriValidationService uriValidator;
  @Autowired
  private ValidationConfig config;

  /**
   * Validate that the requested shape version exists
   */
  private void validateShapeVersion(String shapeVersion) {
    if (shapeVersion == null || shapeVersion.trim().isEmpty()) {
      throw new IllegalArgumentException("Shape version cannot be null or empty");
    }
    
    // Check if at least one shape file exists for this version
    String testShapePath = "shapes/" + shapeVersion + "/platform.ttl";
    try (InputStream testStream = getClass().getClassLoader().getResourceAsStream(testShapePath)) {
      if (testStream == null) {
        throw new IllegalArgumentException("Invalid shape version '" + shapeVersion + "'. Shape directory 'shapes/" + shapeVersion + "/' does not exist or contains no shape files.");
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Error validating shape version '" + shapeVersion + "': " + e.getMessage());
    } catch (Exception e) {
      if (e instanceof IllegalArgumentException) {
        throw e;
      }
      throw new IllegalArgumentException("Error validating shape version '" + shapeVersion + "': " + e.getMessage());
    }
  }

  @Autowired
  private ShaclDiagnosticService diagnosticService;

  /**
   * Core validation method
   */
  public boolean validate(String orderId, InputStream rdfData, boolean strictMode) throws Exception {
    return validate(orderId, rdfData, strictMode, "v2");
  }

  public boolean validate(String orderId, InputStream rdfData, boolean strictMode, String shapeVersion) throws Exception {
    logger.info("[VALIDATION] Validating order {} (strict: {}, shape: {})", orderId, strictMode, shapeVersion);
    
    // Validate shape version first
    validateShapeVersion(shapeVersion);

    ShaclSail sail = null;
    SailRepository repo = null;
    
    try {
      sail = createShaclSail();
      repo = new SailRepository(sail);
      repo.init();

      try (RepositoryConnection conn = repo.getConnection()) {
        conn.begin();

        // Load shapes into SHACL graph
        loadShapes(conn, strictMode, shapeVersion);

        // Load and validate data
        loadData(conn, rdfData, orderId);

        conn.commit();
        
        // Force clear connection cache for large datasets
        conn.clear();
        
        logger.info("Validation passed for order {} (strict: {})", orderId, strictMode);
        return true;
      } catch (ShaclSailValidationException e) {
        int violations = countViolations(e);
        logger.warn("[INVALID] Validation failed for order {} (strict: {}): {} violations", orderId, strictMode, violations);
        logViolations(e, orderId);
        return false;
      }
    } finally {
      // Ensure proper cleanup of resources
      cleanupResources(repo, sail, orderId);
    }
  }

  /**
   * Core validation method with detailed diagnostics
   */
  public ShaclDiagnosticService.ValidationDiagnostics validateWithDiagnostics(String orderId, InputStream rdfData, boolean strictMode) throws Exception {
    return validateWithDiagnostics(orderId, rdfData, strictMode, "v2");
  }

  public ShaclDiagnosticService.ValidationDiagnostics validateWithDiagnostics(String orderId, InputStream rdfData, boolean strictMode, String shapeVersion) throws Exception {
    logger.info("[VALIDATION] Validating order {} with diagnostics (strict: {}, shape: {})", orderId, strictMode, shapeVersion);
    
    // Validate shape version first
    validateShapeVersion(shapeVersion);
    
    long startTime = System.currentTimeMillis();
    List<ShaclDiagnosticService.ViolationDetail> violations = new ArrayList<>();
    boolean isValid = false;
    String errorMessage = null;
    String validationReport = null;

    ShaclSail sail = null;
    SailRepository repo = null;

    try {
      sail = createShaclSail();
      repo = new SailRepository(sail);
      repo.init();

      try (RepositoryConnection conn = repo.getConnection()) {
        conn.begin();

        // Load shapes into SHACL graph
        loadShapes(conn, strictMode, shapeVersion);

        // Load and validate data
        loadData(conn, rdfData, orderId);

        conn.commit();
        
        // Force clear connection cache for large datasets
        conn.clear();
        
        isValid = true;
        logger.info("Validation passed for order {} (strict: {})", orderId, strictMode);

      } catch (ShaclSailValidationException e) {
        isValid = false;
        violations = diagnosticService.extractViolationDetails(e);
        validationReport = diagnosticService.getValidationReport(e);
        errorMessage = "SHACL validation failed with " + violations.size() + " violations";
        
        logger.warn(" Validation failed for order {} (strict: {}): {} violations", orderId, strictMode, violations.size());
        diagnosticService.logViolationsToConsole(orderId, violations);

      } catch (Exception e) {
        isValid = false;
        
        // Check if the exception contains a ShaclSailValidationException
        ShaclSailValidationException shaclException = findShaclException(e);
        if (shaclException != null) {
          logger.info("[DEBUG] Found wrapped ShaclSailValidationException, extracting violations...");
          violations = diagnosticService.extractViolationDetails(shaclException);
          validationReport = diagnosticService.getValidationReport(shaclException);
          errorMessage = "SHACL validation failed with " + violations.size() + " violations";
          diagnosticService.logViolationsToConsole(orderId, violations);
        } else {
          errorMessage = "Validation error: " + e.getMessage();
          logger.error("[FAILURE] Validation error for order: {}", orderId, e);
        }
      }
      
    } finally {
      // Ensure proper cleanup of resources
      cleanupResources(repo, sail, orderId);
    }

    long duration = System.currentTimeMillis() - startTime;
    
    return new ShaclDiagnosticService.ValidationDiagnostics(
      orderId,
      isValid,
      violations,
      duration,
      strictMode,
      errorMessage,
      validationReport
    );
  }

  private ShaclSail createShaclSail() {
    // Create optimized MemoryStore for large datasets
    MemoryStore memoryStore = new MemoryStore();
    memoryStore.setSyncDelay(0); // Immediate sync for faster processing
    
    ShaclSail sail = new ShaclSail(memoryStore);
    // High-performance settings for large RDF files (1-2GB) with memory management
    sail.setParallelValidation(true);   // [KEEP] parallel processing for speed
    sail.setLogValidationPlans(false); 
    sail.setLogValidationViolations(false); 
    sail.setCacheSelectNodes(false);   // [CRITICAL] Disable caching to prevent memory retention
    sail.setGlobalLogValidationExecution(false);
    sail.setEclipseRdf4jShaclExtensions(false);
    sail.setSerializableValidation(false); 
    // Additional performance optimizations
    sail.setPerformanceLogging(false); // Disable performance logging to save memory
    return sail;
  }

  private void loadShapes(RepositoryConnection conn, boolean strictMode, String shapeVersion) throws Exception {
    String[] shapeFiles = getShapeFiles(strictMode, shapeVersion);
    logger.info("[SHAPES] Loading {} shape files (strict: {}, shape: {}): {}", shapeFiles.length, strictMode, shapeVersion,
        String.join(", ", shapeFiles));

    for (String file : shapeFiles) {
      InputStream stream = null;
      InputStream fixed = null;
      try {
        stream = getClass().getClassLoader().getResourceAsStream(file);
        if (stream != null) {
          fixed = uriValidator.fixUriEncoding(stream, file);
          conn.add(fixed, file, RDFFormat.TURTLE, SHACL_SHAPE_GRAPH);
          logger.info("✓ Loaded shape file into SHACL graph: {}", file);
        } else {
          logger.warn("[WARNING] Shape file not found on classpath: {}", file);
        }
      } finally {
        // Ensure shape file streams are properly closed
        if (fixed != null && fixed != stream) {
          try {
            fixed.close();
          } catch (Exception e) {
            logger.debug("Warning: Could not close fixed shape stream for {}: {}", file, e.getMessage());
          }
        }
        if (stream != null) {
          try {
            stream.close();
          } catch (Exception e) {
            logger.debug("Warning: Could not close shape stream for {}: {}", file, e.getMessage());
          }
        }
      }
    }
  }

  private void loadData(RepositoryConnection conn, InputStream rdfData, String orderId) throws Exception {
    InputStream fixed = null;
    try {
      fixed = uriValidator.fixUriEncoding(rdfData, "order-" + orderId);
      conn.add(fixed, RDFFormat.TURTLE);
      logger.debug("✓ Loaded RDF data for order {}", orderId);
    } finally {
      // Ensure streams are properly closed to prevent memory leaks
      if (fixed != null && fixed != rdfData) {
        try {
          fixed.close();
        } catch (Exception e) {
          logger.debug("Warning: Could not close fixed stream for order {}: {}", orderId, e.getMessage());
        }
      }
      if (rdfData != null) {
        try {
          rdfData.close();
        } catch (Exception e) {
          logger.debug("Warning: Could not close RDF data stream for order {}: {}", orderId, e.getMessage());
        }
      }
    }
  }

  private String[] getShapeFiles(boolean strictMode, String shapeVersion) {
    List<String> files = new ArrayList<>();

    // Always load platform shapes
    if (config.isEnablePlatformValidation()) {
      files.add("shapes/" + shapeVersion + "/platform.ttl");
      logger.debug("📄 Adding platform shapes");
    }

    // Git-specific shapes
    if (config.isEnableGitValidation()) {
      files.add("shapes/" + shapeVersion + "/git.ttl");
      logger.debug("📄 Adding Git shapes");
    }

    // GitHub-specific shapes
    if (config.isEnableGithubValidation()) {
      files.add("shapes/" + shapeVersion + "/github.ttl");
      logger.debug("📄 Adding GitHub shapes");
    }

    // Analysis shapes
    if (config.isEnableAnalysisValidation()) {
      files.add("shapes/" + shapeVersion + "/analysis.ttl");
      logger.debug("📄 Adding analysis shapes");
    }

    // Add strict validations ONLY when strict mode is enabled
    if (strictMode) {
      files.add("shapes/" + shapeVersion + "/strict-validations.ttl");
      logger.info("[STRICT] STRICT MODE: Adding strict validation shapes");
    } else {
      logger.info("[STANDARD] STANDARD MODE: Skipping strict validation shapes");
    }

    logger.info("[SHAPES] Final shape files list (strict: {}): {}", strictMode, files);
    return files.toArray(new String[0]);
  }

  private int countViolations(ShaclSailValidationException e) {
    return e.validationReportAsModel().filter(null, SHACL.RESULT, null).size();
  }

  private void logViolations(ShaclSailValidationException e, String orderId) {
    if (logger.isDebugEnabled()) {
      try {
        Model report = e.validationReportAsModel();
        StringWriter writer = new StringWriter();
        Rio.write(report, writer, RDFFormat.TURTLE);
        logger.debug("Validation report for {}:\n{}", orderId, writer.toString());
      } catch (Exception ex) {
        logger.warn("Could not serialize validation report for {}", orderId);
      }
    }
  }

  private ShaclSailValidationException findShaclException(Throwable throwable) {
    if (throwable == null) {
      return null;
    }
    
    if (throwable instanceof ShaclSailValidationException) {
      return (ShaclSailValidationException) throwable;
    }
    
    // Check the cause recursively
    return findShaclException(throwable.getCause());
  }

  /**
   * Aggressive cleanup of RDF4J resources for large datasets (1-2GB RDF files)
   */
  private void cleanupResources(SailRepository repo, ShaclSail sail, String orderId) {
    try {
      logger.info("[CLEANUP] Starting aggressive cleanup for large dataset validation: {}", orderId);
      
      // Step 1: Force close all connections first
      if (repo != null && repo.isInitialized()) {
        try {
          // Get all connections and close them explicitly
          logger.debug("[CLEANUP] Forcing connection closure for order: {}", orderId);
          repo.shutDown();
        } catch (Exception e) {
          logger.warn("[WARNING] Error shutting down repository for {}: {}", orderId, e.getMessage());
        }
      }

      // Step 2: Shutdown the SHACL sail and underlying MemoryStore
      if (sail != null) {
        try {
          logger.debug("[CLEANUP] Shutting down SHACL sail for order: {}", orderId);
          sail.shutDown();
          
          // Additional cleanup for MemoryStore if accessible
          if (sail.getBaseSail() instanceof MemoryStore memoryStore) {
            logger.debug("[CLEANUP] Shutting down underlying MemoryStore for order: {}", orderId);
            memoryStore.shutDown();
          }
        } catch (Exception e) {
          logger.warn("[WARNING] Error shutting down sail for {}: {}", orderId, e.getMessage());
        }
      }

      // Step 3: Clear references and suggest GC for large datasets
      repo = null;
      sail = null;
      
      // For large datasets (1-2GB), suggest GC to free memory immediately
      logger.debug("[CLEANUP] Suggesting garbage collection for large dataset: {}", orderId);
      System.gc();
      
      // Give GC a moment to work
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      
      logger.info("[SUCCESS] Aggressive cleanup completed for order: {}", orderId);
      
    } catch (Exception e) {
      logger.warn("[WARNING] Error during aggressive cleanup for order {}: {}", orderId, e.getMessage());
      // Don't rethrow - cleanup errors shouldn't fail the validation
    }
  }
}
