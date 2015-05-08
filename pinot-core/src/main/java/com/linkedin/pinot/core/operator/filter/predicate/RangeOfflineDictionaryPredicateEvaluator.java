package com.linkedin.pinot.core.operator.filter.predicate;

import com.linkedin.pinot.core.common.predicate.RangePredicate;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;


public class RangeOfflineDictionaryPredicateEvaluator extends AbstractPredicateEvaluator {

  public RangeOfflineDictionaryPredicateEvaluator(RangePredicate predicate, ImmutableDictionaryReader dictionary) {
    int rangeStartIndex = 0;
    int rangeEndIndex = 0;

    final boolean incLower = predicate.includeLowerBoundary();
    final boolean incUpper = predicate.includeUpperBoundary();
    final String lower = predicate.getLowerBoundary();
    final String upper = predicate.getUpperBoundary();

    if (lower.equals("*")) {
      rangeStartIndex = 0;
    } else {
      rangeStartIndex = dictionary.indexOf(lower);
    }
    if (upper.equals("*")) {
      rangeEndIndex = dictionary.length() - 1;
    } else {
      rangeEndIndex = dictionary.indexOf(upper);
    }

    if (rangeStartIndex < 0) {
      rangeStartIndex = -(rangeStartIndex + 1);
    } else if (!incLower && !lower.equals("*")) {
      rangeStartIndex += 1;
    }

    if (rangeEndIndex < 0) {
      rangeEndIndex = -(rangeEndIndex + 1) - 1;
    } else if (!incUpper && !upper.equals("*")) {
      rangeEndIndex -= 1;
    }

    int size = (rangeEndIndex - rangeStartIndex) + 1;
    if (size < 0) {
      size = 0;
    }

    matchingIds = new int[size];

    int counter = 0;
    for (int i = rangeStartIndex; i <= rangeEndIndex; i++) {
      matchingIds[counter++] = i;
    }

  }
}
