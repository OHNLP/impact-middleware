package org.ohnlp.cat.controllers;

import org.ohnlp.cat.dto.JobInfoDTO;
import org.ohnlp.cat.persistence.JDBCBackedStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.UUID;

@Controller
@RequestMapping("/_jobs")
public class JobController {
    // TODO handle backends...
    private final JDBCBackedStorage storage;

    @Autowired
    public JobController(JDBCBackedStorage storage) {
        this.storage = storage;
    }

    @PostMapping("/create")
    public @ResponseBody
    JobInfoDTO createJob(Authentication authentication, @RequestParam("project_uid") String projectUID) {
        JobInfoDTO jobInfo;
        try {
            jobInfo = storage.createJobRecord(authentication, UUID.fromString(projectUID));
        } catch (Throwable e) {
            // TODO log the IOException
            throw new RuntimeException("Error occurred on job creation");
        }
        // TODO actually trigger job (abstractified launcher for beam script)
        return jobInfo;
    }

}
