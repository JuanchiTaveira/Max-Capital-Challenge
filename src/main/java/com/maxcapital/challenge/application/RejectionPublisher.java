package com.maxcapital.challenge.application;

public interface RejectionPublisher {
  void publish(QuarantineStore.Entry entry);
}
