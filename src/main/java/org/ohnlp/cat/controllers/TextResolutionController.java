package org.ohnlp.cat.controllers;

import io.swagger.v3.oas.annotations.tags.Tag;
import org.ohnlp.cat.api.criteria.ClinicalEntityType;
import org.ohnlp.cat.api.criteria.parser.DataSourceRepresentation;
import org.ohnlp.cat.api.ehr.DataSourceInformation;
import org.ohnlp.cat.persistence.JDBCBackedStorage;
import org.ohnlp.cat.textres.TextResolutionService;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Tag(
        name="Text Resolution Controller",
        description="Handles parsing, creation, retrieval, and management of data source representations associated " +
                "with criterion textual descriptions (typically associated with a FHIR CodeableConcept retrieval)")
@Controller
@RequestMapping("/_textres")
public class TextResolutionController {
    public final TextResolutionService textRes;
    public final JDBCBackedStorage storage;

    public TextResolutionController(TextResolutionService textRes, JDBCBackedStorage storage) {
        this.textRes = textRes;
        this.storage = storage;
    }

    // TODO nicer return representation
    @GetMapping("/representations")
    public @ResponseBody
    Map<String, Map<String, Map<String, Collection<DataSourceRepresentation>>>> getRepresentationsForText(
            Authentication authentication,
            @RequestParam(name="project_uid") UUID uid,
            @RequestParam(name="text") String text,
            @RequestParam(name="type") ClinicalEntityType entityType) {
        try {
            List<DataSourceInformation> activeDataSources = storage.getProjectDataSources(authentication, uid);
            return textRes.getDataSourceRepresentations(text, entityType, activeDataSources.stream().map(DataSourceInformation::getBackendID).collect(Collectors.toList()));
        } catch (Throwable e) {
            // TODO log the IOException
            throw new RuntimeException("Error occurred on representation resolution");
        }
    }
}
