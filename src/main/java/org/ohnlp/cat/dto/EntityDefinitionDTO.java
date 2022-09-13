package org.ohnlp.cat.dto;

import org.ohnlp.cat.dto.enums.ClinicalEntityType;

public class EntityDefinitionDTO {
    public ClinicalEntityType type;
    public ValueDefinitionDTO[] definitionComponents;

    public ValueDefinitionDTO[] getDefinitionComponents() {
        return definitionComponents;
    }

    public void setDefinitionComponents(ValueDefinitionDTO[] definitionComponents) {
        this.definitionComponents = definitionComponents;
    }
}
