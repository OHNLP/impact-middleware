package org.ohnlp.cat.dto;

import java.util.UUID;

public class ProjectDTO {
    private UUID uid;
    private String name;

    public UUID getUid() {
        return uid;
    }

    public void setUid(UUID uid) {
        this.uid = uid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
