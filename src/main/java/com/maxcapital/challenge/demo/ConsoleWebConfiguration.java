package com.maxcapital.challenge.demo;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
@Profile("evaluation-console")
public class ConsoleWebConfiguration implements WebMvcConfigurer {
  private final ConsoleMutationInterceptor mutations;

  public ConsoleWebConfiguration(ConsoleMutationInterceptor mutations) {
    this.mutations = mutations;
  }

  @Override
  public void addInterceptors(InterceptorRegistry registry) {
    registry.addInterceptor(mutations).addPathPatterns("/demo/api/**");
  }

  @Override
  public void addResourceHandlers(ResourceHandlerRegistry registry) {
    registry.addResourceHandler("/assets/**").addResourceLocations("classpath:/demo/");
  }
}
