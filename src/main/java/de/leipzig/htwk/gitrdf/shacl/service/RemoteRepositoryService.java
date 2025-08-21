package de.leipzig.htwk.gitrdf.shacl.service;

import java.io.InputStream;
import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import de.leipzig.htwk.gitrdf.shacl.model.RdfDataResult;

/**
 * Service for fetching RDF data from different remote repository platforms
 */
@Service
public class RemoteRepositoryService {

  private static final Logger logger = LoggerFactory.getLogger(RemoteRepositoryService.class);

  @Autowired
  private RestTemplate restTemplate;

  @Value("${services.listener.api.github-rdf:http://listener-service:8080/listener-service}")
  private String listenerServiceBaseUrl;

  /**
   * Fetch RDF data for a GitHub repository order
   */
  public RdfDataResult fetchGithubRdf(String orderId) throws Exception {
    String endpoint = "/api/v1/github/rdf/download/" + orderId;
    return fetchRdfFromEndpoint("GitHub", orderId, endpoint);
  }


  /**
   * Check if RDF data is available for a GitHub repository
   */
  public boolean isGithubRdfAvailable(String orderId) {
    return checkRdfAvailability("GitHub", orderId, "/api/v1/github/rdf/download/" + orderId);
  }


  /**
   * Generic method to fetch RDF from any endpoint
   */
  private RdfDataResult fetchRdfFromEndpoint(String platform, String orderId, String endpoint) throws Exception {
    logger.debug("[FETCH] Fetching {} RDF data for order: {}", platform, orderId);

    String url = listenerServiceBaseUrl + endpoint;
    logger.debug("[REQUEST] Requesting {} RDF from: {}", platform, url);

    try {
      ResponseEntity<byte[]> response = restTemplate.getForEntity(URI.create(url), byte[].class);
      byte[] rdfData = response.getBody();
      if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null && rdfData != null) {
        logger.info("[SUCCESS] Successfully fetched {} RDF data ({} bytes) for order: {}",
            platform, rdfData.length, orderId);

        InputStream stream = new java.io.ByteArrayInputStream(rdfData);
        return new RdfDataResult(stream, platform, orderId,
            String.format("Fetched %s RDF data (%d bytes)", platform, rdfData.length));
    
      } else {
        logger.warn("[WARNING] No {} RDF data available for order: {} (status: {})",
            platform, orderId, response.getStatusCode());
        throw new Exception(String.format("No %s RDF data available for order: %s", platform, orderId));
      }

    } catch (Exception e) {
      logger.error("[ERROR] Failed to fetch {} RDF from listener service for order: {}", platform, orderId, e);
      throw new Exception(String.format("Failed to fetch %s RDF for order %s: %s",
          platform, orderId, e.getMessage()), e);
    }
  }

  /**
   * Generic method to check RDF availability
   */
  private boolean checkRdfAvailability(String platform, String orderId, String endpoint) {
    try {
      String url = listenerServiceBaseUrl + endpoint;
      ResponseEntity<Void> response = restTemplate.getForEntity(URI.create(url), Void.class);
      boolean available = response.getStatusCode() == HttpStatus.OK;
      logger.debug("[STATS] {} RDF availability for order {}: {}", platform, orderId, available);
      return available;
    } catch (Exception e) {
      logger.debug("[STATS] {} RDF not available for order: {} ({})", platform, orderId, e.getMessage());
      return false;
    }
  }

}