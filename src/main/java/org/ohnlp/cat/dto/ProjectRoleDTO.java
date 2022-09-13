package org.ohnlp.cat.dto;

import org.ohnlp.cat.dto.enums.ProjectAuthorityGrant;

import java.util.UUID;

public class ProjectRoleDTO {
    private String user_uid;
    private UUID project_uid;
    private ProjectAuthorityGrant grant;

    public String getUser_uid() {
        return user_uid;
    }

    public void setUser_uid(String user_uid) {
        this.user_uid = user_uid;
    }

    public UUID getProject_uid() {
        return project_uid;
    }

    public void setProject_uid(UUID project_uid) {
        this.project_uid = project_uid;
    }

    public ProjectAuthorityGrant getGrant() {
        return grant;
    }

    public void setGrant(ProjectAuthorityGrant grant) {
        this.grant = grant;
    }
}
