package org.ember.TuGraphFinbench.Record;

import lombok.ToString;

@ToString
public enum EdgeType {
    Transfer,
    Deposit,
    Apply,
    Guarantee,
    Own
}
