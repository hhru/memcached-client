package ru.hh.memcached;

public class CASPair <T> {
  public final long casID;
  public final T value;

  public CASPair(long casID, T value) {
    this.casID = casID;
    this.value = value;
  }
}
