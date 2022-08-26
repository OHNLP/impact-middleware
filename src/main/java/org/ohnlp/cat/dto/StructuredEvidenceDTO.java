package org.ohnlp.cat.dto;

import java.util.Date;
import java.util.UUID;

public class StructuredEvidenceDTO {
    private UUID evidence_uid;
    private String code_system;
    private String code;
    private String desc;
    private Date dtm;
    private UUID criteria_definition_uid;
    private double score;
}
