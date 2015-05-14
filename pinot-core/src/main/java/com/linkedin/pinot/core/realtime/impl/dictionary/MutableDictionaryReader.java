/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.realtime.impl.dictionary;

import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public abstract class MutableDictionaryReader implements Dictionary {
  protected BiMap<Integer, Object> dictionaryIdBiMap;
  protected FieldSpec spec;
  protected boolean hasNull = false;
  private final AtomicInteger dictionaryIdGenerator;

  public MutableDictionaryReader(FieldSpec spec) {
    this.spec = spec;
    this.dictionaryIdBiMap = HashBiMap.<Integer, Object> create();
    dictionaryIdGenerator = new AtomicInteger(-1);
  }

  protected void addToDictionaryBiMap(Object val) {
    if (!dictionaryIdBiMap.inverse().containsKey(val)) {
      dictionaryIdBiMap.put(new Integer(dictionaryIdGenerator.incrementAndGet()), val);
      return;
    }
  }

  @Override
  public int length() {
    return dictionaryIdGenerator.get() + 1;
  }

  protected Integer getIndexOfFromBiMap(Object val) {
    Integer ret = dictionaryIdBiMap.inverse().get(val);
    if (ret == null) {
      ret = -1;
    }
    return ret;
  }

  protected Object getRawValueFromBiMap(int dictionaryId) {
    return dictionaryIdBiMap.get(new Integer(dictionaryId));
  }

  public boolean hasNull() {
    return hasNull;
  }

  public abstract Object getMinVal();

  public abstract Object getMaxVal();

  public abstract void index(Object rawValue);

  @Override
  public abstract int indexOf(Object rawValue);

  public abstract boolean contains(Object o);

  @Override
  public abstract Object get(int dictionaryId);

  public abstract boolean inRange(String lower, String upper, int indexOfValueToCompare, boolean includeLower,
      boolean includeUpper);

  public boolean inRange(String lower, String upper, int valueToCompare) {
    return inRange(lower, upper, valueToCompare, true, true);
  }

  @Override
  public abstract long getLongValue(int dictionaryId);

  @Override
  public abstract double getDoubleValue(int dictionaryId);

  @Override
  public abstract String toString(int dictionaryId);

  public void print() {
    System.out.println("************* printing dictionary for column : " + spec.getName() + " ***************");
    for (Integer key : dictionaryIdBiMap.keySet()) {
      System.out.println(key + "," + dictionaryIdBiMap.get(key));
    }
    System.out.println("************************************");
  }
}
