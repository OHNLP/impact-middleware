package org.ohnlp.cat.dto;

import org.ohnlp.cat.dto.enums.JobStatus;

import java.util.Date;
import java.util.UUID;

public class JobInfoDTO {
    private UUID project_uid;
    private UUID job_uid;
    private Date startDate;
    private JobStatus status;

    public UUID getProject_uid() {
        return project_uid;
    }

    public void setProject_uid(UUID project_uid) {
        this.project_uid = project_uid;
    }

    public UUID getJob_uid() {
        return job_uid;
    }

    public void setJob_uid(UUID job_uid) {
        this.job_uid = job_uid;
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public JobStatus getStatus() {
        return status;
    }

    public void setStatus(JobStatus status) {
        this.status = status;
    }
}
