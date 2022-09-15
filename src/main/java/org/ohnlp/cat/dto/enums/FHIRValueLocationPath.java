package org.ohnlp.cat.dto.enums;

public enum FHIRValueLocationPath {
    CONDITION_CODE("code.coding.code"),
    PROCEDURE_CODE("code.coding.code"),
    MEDICATION_CODE("medication.coding.code"),
    OBSERVATION_CODE("code.coding.code"),
    OBSERVATION_VALUE("value.value");

    private final String path;

    FHIRValueLocationPath(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }
}