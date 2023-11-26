package org.ember.TuGraphFinbench.Algorithms;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
public class Case4Message {
    double layer0LoanAmountSum = 0;
    double layer1LoanAmountSum = 0;
    double layer2LoanAmountSum = 0;
    double layer3LoanAmountSum = 0;
    int highestLayer = 0;
    Map<Long, Double> layer0LoanAmountMap;
    Map<Long, Double> layer1LoanAmountMap;
    Map<Long, Double> layer2LoanAmountMap;
    Map<Long, Double> layer3LoanAmountMap;
}
