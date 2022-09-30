package org.ohnlp.cat.controllers;

import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Tag(
        name="Terminology Controller",
        description="Handles parsing, creation, retrieval, and management of codesets associated with criterion " +
                "textual descriptions (typically associated with a FHIR CodeableConcept retrieval)")
@Controller
@RequestMapping("/_terminology")
public class TerminologyController {

}
