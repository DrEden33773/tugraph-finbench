package org.ember.TuGraphFinbench.Cell;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.jetbrains.annotations.NotNull;

@Data
@AllArgsConstructor
public class Case4Cell implements Comparable<Case4Cell> {
    long ID;
    double highestLayerLoanAmountSum;

    @Override
    public int compareTo(@NotNull Case4Cell o) {
        return Long.compare(this.ID, o.ID);
    }

    @Override
    public String toString() {
        return this.ID + "|" + String.format("%.2f", this.highestLayerLoanAmountSum);
    }
}
