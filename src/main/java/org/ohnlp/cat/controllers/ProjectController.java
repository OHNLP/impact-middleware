package org.ohnlp.cat.controllers;

import org.ohnlp.cat.dto.ProjectDTO;
import org.ohnlp.cat.persistence.JDBCBackedStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;

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
    List<ProjectDTO> getProjectList(Authentication authentication) {
        return storage.getProjectList(authentication);
    }
}
