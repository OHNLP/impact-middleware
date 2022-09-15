package org.ohnlp.cat.dto;

import org.ohnlp.cat.dto.enums.JudgementState;

public class JudgementDTO {
    private JudgementState judgement;
    private String comment;

    public JudgementState getJudgement() {
        return judgement;
    }

    public void setJudgement(JudgementState judgement) {
        this.judgement = judgement;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }
}
