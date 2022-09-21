package org.ohnlp.cat.controllers;

import org.ohnlp.cat.api.criteria.Criterion;
import org.ohnlp.cat.api.projects.Project;
import org.ohnlp.cat.api.projects.ProjectRole;
import org.ohnlp.cat.persistence.JDBCBackedStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@Controller
@RequestMapping("/_projects")
public class ProjectController {

    private final JDBCBackedStorage storage;

    @Autowired
    public ProjectController(JDBCBackedStorage storage) {
        this.storage = storage;
    }

    @GetMapping("/")
    public @ResponseBody
    List<Project> getProjectList(Authentication authentication) {
        try {
            return storage.getProjectList(authentication);
        } catch (Throwable e) {
            // TODO log the IOException
            throw new RuntimeException("Error occurred on project list retrieve");
        }
    }

    @PutMapping("/create")
    public @ResponseBody
    Project createProject(Authentication authentication, @RequestParam(name="name") String projectName) {
        try {
            return storage.createProject(authentication, projectName);
        } catch (Throwable e) {
            // TODO log the IOException
            throw new RuntimeException("Error occurred on project creation");
        }
    }

    @PostMapping("/rename")
    public @ResponseBody
    Project renameProject(Authentication authentication, @RequestParam(name="project_name") String projectName, @RequestParam(name="project_uid") UUID uid) {
        try {
            return storage.renameProject(authentication, uid, projectName);
        } catch (Throwable e) {
            // TODO log the IOException
            throw new RuntimeException("Error occurred on project rename");
        }
    }

    @PostMapping("/roles")
    public @ResponseBody
    Boolean setUserRole(Authentication authentication, @RequestBody ProjectRole roleDef) {
        try {
            return storage.updateRoleGrants(authentication, roleDef);
        } catch (Throwable e) {
            // TODO log the IOException
            throw new RuntimeException("Error occurred on project rename");
        }
    }

    @DeleteMapping("/archive")
    public @ResponseBody
    Boolean archiveProject(Authentication authentication, @RequestParam(name="project_uid") UUID uid) {
        try {
            return storage.archiveProject(authentication, uid);
        } catch (Throwable e) {
            // TODO log the IOException
            throw new RuntimeException("Error occurred on project rename");
        }
    }

    @GetMapping("/criterion")
    public @ResponseBody
    Criterion getProjectCriterion(Authentication authentication, @RequestParam(name="project_uid") UUID uid) {
        try {
            return storage.getProjectCriterion(authentication, uid);
        } catch (Throwable e) {
            // TODO log the IOException
            throw new RuntimeException("Error occurred on project rename");
        }
    }

    @PostMapping("/criterion")
    public @ResponseBody
    Boolean writeProjectCriterion(Authentication authentication, @RequestParam(name="project_uid") UUID uid, @RequestBody Criterion criterion) {
        try {
            return storage.writeProjectCriterion(authentication, uid, criterion);
        } catch (Throwable e) {
            // TODO log the IOException
            throw new RuntimeException("Error occurred on project rename");
        }
    }
}
