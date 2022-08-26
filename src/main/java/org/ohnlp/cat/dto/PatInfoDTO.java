package org.ohnlp.cat.dto;

import org.ohnlp.cat.dto.enums.CohortInclusion;

import java.util.Date;

public class PatInfoDTO {
    private String pat_id;
    private String name;
    private CohortInclusion inclusion;
    private Date dob;
    private boolean birth_gender_male_flag;

    public String getPat_id() {
        return pat_id;
    }

    public void setPat_id(String pat_id) {
        this.pat_id = pat_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public CohortInclusion getInclusion() {
        return inclusion;
    }

    public void setInclusion(CohortInclusion inclusion) {
        this.inclusion = inclusion;
    }

    public Date getDob() {
        return dob;
    }

    public void setDob(Date dob) {
        this.dob = dob;
    }

    public boolean isBirth_gender_male_flag() {
        return birth_gender_male_flag;
    }

    public void setBirth_gender_male_flag(boolean birth_gender_male_flag) {
        this.birth_gender_male_flag = birth_gender_male_flag;
    }
}
