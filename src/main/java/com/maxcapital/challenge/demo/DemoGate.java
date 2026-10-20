package com.maxcapital.challenge.demo;

import java.util.function.Supplier;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

@Component
@Profile("evaluation-console")
public class DemoGate {
  private String active;

  public synchronized <T> T manual(Supplier<T> action) {
    if (active != null)
      throw new ResponseStatusException(HttpStatus.CONFLICT, "Hay un escenario activo: " + active);
    return action.get();
  }

  synchronized void acquire(String id) {
    if (active != null)
      throw new ResponseStatusException(HttpStatus.CONFLICT, "Hay un escenario activo: " + active);
    active = id;
  }

  synchronized void release(String id) {
    if (id.equals(active)) active = null;
  }

  public synchronized String active() {
    return active;
  }
}
