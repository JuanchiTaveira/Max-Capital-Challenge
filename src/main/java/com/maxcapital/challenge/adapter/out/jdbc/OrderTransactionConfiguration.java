package com.maxcapital.challenge.adapter.out.jdbc;

import javax.sql.DataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.support.JdbcTransactionManager;

@Configuration
public class OrderTransactionConfiguration {
  @Bean
  JdbcTransactionManager orderTransactionManager(DataSource dataSource) {
    return new JdbcTransactionManager(dataSource);
  }
}
