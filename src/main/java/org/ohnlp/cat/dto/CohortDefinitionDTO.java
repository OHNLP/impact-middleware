package org.ohnlp.cat.dto;

import org.ohnlp.cat.dto.enums.NodeType;
import org.ohnlp.cat.dto.enums.ValueType;

import java.util.List;
import java.util.UUID;

public class CohortDefinitionDTO {
    private UUID node_id;
    private NodeType node_type;
    private ValueType value_type;
    private CohortDefinitionValueDTO value;
    private List<CohortDefinitionDTO> children;

    public UUID getNode_id() {
        return node_id;
    }

    public void setNode_id(UUID node_id) {
        this.node_id = node_id;
    }

    public NodeType getNode_type() {
        return node_type;
    }

    public void setNode_type(NodeType node_type) {
        this.node_type = node_type;
    }

    public ValueType getValue_type() {
        return value_type;
    }

    public void setValue_type(ValueType value_type) {
        this.value_type = value_type;
    }

    public CohortDefinitionValueDTO getValue() {
        return value;
    }

    public void setValue(CohortDefinitionValueDTO value) {
        this.value = value;
    }

    public List<CohortDefinitionDTO> getChildren() {
        return children;
    }

    public void setChildren(List<CohortDefinitionDTO> children) {
        this.children = children;
    }
}
