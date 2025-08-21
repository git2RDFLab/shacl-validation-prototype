package de.leipzig.htwk.gitrdf.shacl.core;

import static org.eclipse.rdf4j.model.vocabulary.RDF4J.SHACL_SHAPE_GRAPH;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sail.shacl.ShaclSail;
import org.eclipse.rdf4j.sail.shacl.ShaclSailValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import de.leipzig.htwk.gitrdf.shacl.model.ShapeDefinition;
import de.leipzig.htwk.gitrdf.shacl.service.RemoteShapeService;
import de.leipzig.htwk.gitrdf.shacl.service.ShaclDiagnosticService;
import de.leipzig.htwk.gitrdf.shacl.service.UriValidationService;

/**
 * Enhanced validation engine that supports both legacy version-based shapes
 * and new JSON-defined remote shapes
 */
@Component
public class EnhancedValidationEngine {
    
    private static final Logger logger = LoggerFactory.getLogger(EnhancedValidationEngine.class);
    
    @Autowired
    private ShaclValidationEngine legacyEngine;
    
    @Autowired
    private RemoteShapeService remoteShapeService;
    
    @Autowired
    private UriValidationService uriValidator;
    
    @Autowired
    private ShaclDiagnosticService diagnosticService;
    
    /**
     * Validate using remote shapes defined in JSON
     */
    public ShaclDiagnosticService.ValidationDiagnostics validateWithRemoteShapes(
            String orderId, InputStream rdfData, boolean strictMode, ShapeDefinition shapeDefinition) throws Exception {
        
        logger.info("[ENHANCED] Validating order {} with {} remote shapes (strict: {})", 
            orderId, shapeDefinition.getShapeCount(), strictMode);
        
        long startTime = System.currentTimeMillis();
        List<ShaclDiagnosticService.ViolationDetail> violations = new ArrayList<>();
        boolean isValid = false;
        String errorMessage = null;
        String validationReport = null;
        
        // Validate and download remote shapes
        Map<String, InputStream> remoteShapes = null;
        ShaclSail sail = null;
        SailRepository repo = null;
        
        try {
            // Download remote shapes
            remoteShapeService.validateShapeUrls(shapeDefinition.getShapes());
            remoteShapes = remoteShapeService.downloadShapes(shapeDefinition.getShapes());
            
            // Create SHACL sail
            sail = createShaclSail();
            repo = new SailRepository(sail);
            repo.init();
            
            try (RepositoryConnection conn = repo.getConnection()) {
                conn.begin();
                
                // Load remote shapes into SHACL graph
                loadRemoteShapes(conn, remoteShapes);
                
                // Load and validate data
                loadData(conn, rdfData, orderId);
                
                conn.commit();
                conn.clear();
                
                isValid = true;
                logger.info("Enhanced validation passed for order {} with remote shapes", orderId);
                
            } catch (ShaclSailValidationException e) {
                isValid = false;
                violations = diagnosticService.extractViolationDetails(e);
                validationReport = diagnosticService.getValidationReport(e);
                errorMessage = "SHACL validation failed with " + violations.size() + " violations";
                
                logger.warn(" Enhanced validation failed for order {}: {} violations", orderId, violations.size());
                diagnosticService.logViolationsToConsole(orderId, violations);
                
            } catch (Exception e) {
                isValid = false;
                ShaclSailValidationException shaclException = findShaclException(e);
                if (shaclException != null) {
                    violations = diagnosticService.extractViolationDetails(shaclException);
                    validationReport = diagnosticService.getValidationReport(shaclException);
                    errorMessage = "SHACL validation failed with " + violations.size() + " violations";
                    diagnosticService.logViolationsToConsole(orderId, violations);
                } else {
                    errorMessage = "Enhanced validation error: " + e.getMessage();
                    logger.error("[FAILURE] Enhanced validation error for order: {}", orderId, e);
                }
            }
            
        } finally {
            // Cleanup remote shape streams
            if (remoteShapes != null) {
                for (Map.Entry<String, InputStream> entry : remoteShapes.entrySet()) {
                    try {
                        entry.getValue().close();
                        logger.debug("🧹 Closed remote shape stream: {}", entry.getKey());
                    } catch (Exception e) {
                        logger.warn("⚠️ Could not close remote shape stream {}: {}", entry.getKey(), e.getMessage());
                    }
                }
            }
            
            // Cleanup repository resources
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
    
    /**
     * Validate using legacy version-based shapes (delegates to existing engine)
     */
    public ShaclDiagnosticService.ValidationDiagnostics validateWithLegacyShapes(
            String orderId, InputStream rdfData, boolean strictMode, String shapeVersion) throws Exception {
        
        logger.info("[LEGACY] Using legacy validation for order {} (shape: {})", orderId, shapeVersion);
        return legacyEngine.validateWithDiagnostics(orderId, rdfData, strictMode, shapeVersion);
    }
    
    private ShaclSail createShaclSail() {
        MemoryStore memoryStore = new MemoryStore();
        memoryStore.setSyncDelay(0);
        
        ShaclSail sail = new ShaclSail(memoryStore);
        sail.setParallelValidation(true);
        sail.setLogValidationPlans(false);
        sail.setLogValidationViolations(false);
        sail.setCacheSelectNodes(false);
        sail.setGlobalLogValidationExecution(false);
        sail.setEclipseRdf4jShaclExtensions(false);
        sail.setSerializableValidation(false);
        sail.setPerformanceLogging(false);
        return sail;
    }
    
    private void loadRemoteShapes(RepositoryConnection conn, Map<String, InputStream> remoteShapes) throws Exception {
        logger.info("[SHAPES] Loading {} remote shape files", remoteShapes.size());
        
        for (Map.Entry<String, InputStream> entry : remoteShapes.entrySet()) {
            String shapeName = entry.getKey();
            InputStream shapeStream = entry.getValue();
            
            InputStream fixed = null;
            try {
                fixed = uriValidator.fixUriEncoding(shapeStream, shapeName);
                conn.add(fixed, shapeName, RDFFormat.TURTLE, SHACL_SHAPE_GRAPH);
                logger.info("✓ Loaded remote shape into SHACL graph: {}", shapeName);
            } finally {
                if (fixed != null && fixed != shapeStream) {
                    try {
                        fixed.close();
                    } catch (Exception e) {
                        logger.debug("Warning: Could not close fixed shape stream for {}: {}", shapeName, e.getMessage());
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
            if (fixed != null && fixed != rdfData) {
                try {
                    fixed.close();
                } catch (Exception e) {
                    logger.debug("Warning: Could not close fixed stream for order {}: {}", orderId, e.getMessage());
                }
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
        
        return findShaclException(throwable.getCause());
    }
    
    private void cleanupResources(SailRepository repo, ShaclSail sail, String orderId) {
        try {
            logger.info("[CLEANUP] Starting cleanup for enhanced validation: {}", orderId);
            
            if (repo != null && repo.isInitialized()) {
                try {
                    repo.shutDown();
                } catch (Exception e) {
                    logger.warn("[WARNING] Error shutting down repository for {}: {}", orderId, e.getMessage());
                }
            }
            
            if (sail != null) {
                try {
                    sail.shutDown();
                    if (sail.getBaseSail() instanceof MemoryStore memoryStore) {
                        memoryStore.shutDown();
                    }
                } catch (Exception e) {
                    logger.warn("[WARNING] Error shutting down sail for {}: {}", orderId, e.getMessage());
                }
            }
            
            System.gc();
            Thread.sleep(100);
            
            logger.info("[SUCCESS] Enhanced cleanup completed for order: {}", orderId);
            
        } catch (Exception e) {
            logger.warn("[WARNING] Error during enhanced cleanup for order {}: {}", orderId, e.getMessage());
        }
    }
}