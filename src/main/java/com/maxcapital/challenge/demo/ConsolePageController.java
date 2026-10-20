package com.maxcapital.challenge.demo;

import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Profile("evaluation-console")
public class ConsolePageController {
  @GetMapping(value = "/", produces = MediaType.TEXT_HTML_VALUE)
  public Resource index() {
    return new ClassPathResource("demo/index.html");
  }
}
