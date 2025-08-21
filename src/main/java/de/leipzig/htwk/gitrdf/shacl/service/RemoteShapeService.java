package de.leipzig.htwk.gitrdf.shacl.service;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

/**
 * Service for downloading SHACL shapes from remote URLs
 */
@Service
public class RemoteShapeService {
    
    private static final Logger logger = LoggerFactory.getLogger(RemoteShapeService.class);
    
    @Autowired
    private RestTemplate restTemplate;
    
    /**
     * Download shapes from remote URLs
     * @param shapeDefinitions Map of shape names to URLs
     * @return Map of shape names to downloaded content as InputStreams
     */
    public Map<String, InputStream> downloadShapes(Map<String, String> shapeDefinitions) throws Exception {
        Map<String, InputStream> downloadedShapes = new HashMap<>();
        
        logger.info("[REMOTE_SHAPES] Downloading {} shapes from remote URLs", shapeDefinitions.size());
        
        for (Map.Entry<String, String> entry : shapeDefinitions.entrySet()) {
            String shapeName = entry.getKey();
            String shapeUrl = entry.getValue();
            
            try {
                logger.info("[DOWNLOAD] Downloading shape '{}' from: {}", shapeName, shapeUrl);
                
                // Validate URL
                URI uri = new URI(shapeUrl);
                if (!uri.getScheme().equals("http") && !uri.getScheme().equals("https")) {
                    throw new IllegalArgumentException("Only HTTP and HTTPS URLs are supported: " + shapeUrl);
                }
                
                // Download the shape content
                ResponseEntity<String> response = restTemplate.getForEntity(shapeUrl, String.class);
                
                if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                    String content = response.getBody();
                    InputStream shapeStream = new ByteArrayInputStream(content.getBytes());
                    downloadedShapes.put(shapeName, shapeStream);
                    
                    logger.info("[SUCCESS] Downloaded shape '{}' ({} bytes)", shapeName, content.length());
                } else {
                    throw new RuntimeException("Failed to download shape from " + shapeUrl + 
                        ". Status: " + response.getStatusCode());
                }
                
            } catch (Exception e) {
                logger.error("[ERROR] Failed to download shape '{}' from {}: {}", shapeName, shapeUrl, e.getMessage());
                throw new RuntimeException("Failed to download shape '" + shapeName + "' from " + shapeUrl, e);
            }
        }
        
        logger.info("[COMPLETE] Successfully downloaded {} shapes", downloadedShapes.size());
        return downloadedShapes;
    }
    
    /**
     * Validate that all URLs in the shape definition are accessible
     */
    public void validateShapeUrls(Map<String, String> shapeDefinitions) throws Exception {
        logger.info("[VALIDATION] Validating {} shape URLs", shapeDefinitions.size());
        
        for (Map.Entry<String, String> entry : shapeDefinitions.entrySet()) {
            String shapeName = entry.getKey();
            String shapeUrl = entry.getValue();
            
            try {
                URI uri = new URI(shapeUrl);
                if (!uri.getScheme().equals("http") && !uri.getScheme().equals("https")) {
                    throw new IllegalArgumentException("Only HTTP and HTTPS URLs are supported for shape '" + 
                        shapeName + "': " + shapeUrl);
                }
                
                // Test connectivity with HEAD request
                try {
                    restTemplate.headForHeaders(shapeUrl);
                    logger.debug("[VALID] Shape URL '{}' is accessible: {}", shapeName, shapeUrl);
                } catch (Exception e) {
                    logger.warn("[WARNING] Shape URL '{}' may not be accessible: {} ({})", 
                        shapeName, shapeUrl, e.getMessage());
                    // Don't fail validation on HEAD request issues, as some servers don't support HEAD
                }
                
            } catch (Exception e) {
                logger.error("[INVALID] Invalid URL for shape '{}': {}", shapeName, shapeUrl);
                throw new IllegalArgumentException("Invalid URL for shape '" + shapeName + "': " + shapeUrl, e);
            }
        }
        
        logger.info("[SUCCESS] All shape URLs validated");
    }
}