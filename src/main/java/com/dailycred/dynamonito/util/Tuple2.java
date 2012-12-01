package com.dailycred.dynamonito.util;

import com.google.common.base.Objects;

/**
 * Container to store a pair of objects.
 * 
 * @param <T1>
 * @param <T2>
 */
public class Tuple2<T1, T2> {
  public final T1 _1;
  public final T2 _2;

  public Tuple2(T1 o1, T2 o2) {
    _1 = o1;
    _2 = o2;
  }

  public T1 getKey() {
    return _1;
  }

  public T2 getValue() {
    return _2;
  }

  public static <T1, T2> Tuple2<T1, T2> build(T1 t1, T2 t2) {
    return new Tuple2<T1, T2>(t1, t2);
  }

  @Override
  public String toString() {
    return "(" + _1 + ", " + _2 + ")";
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(_1, _2);
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean equals(Object rhs) {
    if (rhs instanceof Tuple2<?, ?> &&
        ((Tuple2<T1, T2>) rhs)._1 == _1 &&
        ((Tuple2<T1, T2>) rhs)._2 == _2)
      return true;
    return false;
  }
}