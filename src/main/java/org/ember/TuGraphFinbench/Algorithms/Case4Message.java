package org.ember.TuGraphFinbench.Algorithms;

import lombok.AllArgsConstructor;

import java.util.Map;

@AllArgsConstructor
public class Case4Message {
    int highestLayer;
    Map<Long, Double> ahead0LayerLoanAmountMap;
    Map<Long, Double> ahead1LayerLoanAmountMap;
    Map<Long, Double> ahead2LayerLoanAmountMap;
    Map<Long, Double> ahead3LayerLoanAmountMap;
}
