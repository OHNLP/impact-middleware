package org.ohnlp.cat.controllers;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.ohnlp.cat.ApplicationConfiguration;
import org.ohnlp.cat.api.criteria.Criterion;
import org.ohnlp.cat.api.ehr.DataSourceInformation;
import org.ohnlp.cat.api.projects.Project;
import org.ohnlp.cat.api.projects.ProjectRole;
import org.ohnlp.cat.persistence.JDBCBackedStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@Tag(name="Project Controller", description="Operations relating to Project Creation and Management")
@Controller
@RequestMapping("/_projects")
public class ProjectController {

    private final JDBCBackedStorage storage;
    private final ApplicationConfiguration config;

    @Autowired
    public ProjectController(JDBCBackedStorage storage, ApplicationConfiguration config) {
        this.storage = storage;
        this.config = config;
    }

    @Operation(summary="Gets listing of projects to which the calling user has read access")
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

    @Operation(summary="Creates a new project with the given name with the calling user set to owner")
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

    @Operation(summary="Renames the project associated with the given project UID to the given name")
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

    @Operation(summary="Sets the given project authority/role grant")
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

    @Operation(summary="Archives the project associated with the given project UID")
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

    @Operation(summary="Gets the current (latest) revision of a criterion associated with a given project UID. ",
            description="Note that for jobs, the associated method under /_cohorts should be called instead using job UID as a parameter " +
            "as the criterion associated with a prior job may be different from the latest revision")
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

    @Operation(summary="Updates the criterion associated with a given project UID")
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

    @Operation(summary="Gets a listing of available data sources")
    @GetMapping("/available_data_sources")
    public @ResponseBody
    List<DataSourceInformation> getAvailableDataSources() {
        return config.getDataSources();
    }

    @Operation(summary="Gets a listing of data sources currently used for a given project UID")
    @GetMapping("/data_sources")
    public @ResponseBody
    List<DataSourceInformation> getDataSources(Authentication authentication, @RequestParam(name="project_uid") UUID uid) {
        try {
            return storage.getProjectDataSources(authentication, uid);
        } catch (Throwable e) {
            // TODO log the IOException
            throw new RuntimeException("Error occurred on project data source retrieve");
        }
    }

    @Operation(summary="Updates list of data sources associated with a given project")
    @PostMapping("/data_sources")
    public @ResponseBody
    Boolean writeDataSources(Authentication authentication, @RequestParam(name="project_uid") UUID uid,
                           @RequestBody List<DataSourceInformation> dataSources) {
        try {
            return storage.writeProjectDataSources(authentication, uid, dataSources);
        } catch (Throwable e) {
            // TODO log the IOException
            throw new RuntimeException("Error occurred on project data source write");
        }
    }
}
