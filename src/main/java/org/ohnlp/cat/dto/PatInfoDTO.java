package org.ohnlp.cat.dto;

import org.ohnlp.cat.dto.enums.PatientJudgementState;

public class PatInfoDTO {
    private String pat_id;
    private PatientJudgementState inclusion;

    public String getPat_id() {
        return pat_id;
    }

    public void setPat_id(String pat_id) {
        this.pat_id = pat_id;
    }


    public PatientJudgementState getInclusion() {
        return inclusion;
    }

    public void setInclusion(PatientJudgementState inclusion) {
        this.inclusion = inclusion;
    }
}
