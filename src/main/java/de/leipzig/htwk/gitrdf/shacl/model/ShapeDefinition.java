package de.leipzig.htwk.gitrdf.shacl.model;

import java.util.Map;

/**
 * Model for JSON-based shape definition input
 */
public class ShapeDefinition {
    private Map<String, String> shapes;
    
    public ShapeDefinition() {}
    
    public ShapeDefinition(Map<String, String> shapes) {
        this.shapes = shapes;
    }
    
    public Map<String, String> getShapes() {
        return shapes;
    }
    
    public void setShapes(Map<String, String> shapes) {
        this.shapes = shapes;
    }
    
    public boolean hasShapes() {
        return shapes != null && !shapes.isEmpty();
    }
    
    public int getShapeCount() {
        return shapes != null ? shapes.size() : 0;
    }
}