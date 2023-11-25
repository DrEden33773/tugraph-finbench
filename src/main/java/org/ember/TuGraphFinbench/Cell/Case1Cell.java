package org.ember.TuGraphFinbench.Cell;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.jetbrains.annotations.NotNull;

@Data
@AllArgsConstructor
public class Case1Cell implements Comparable<Case1Cell> {
    long ID;
    double loanAmountSum;

    @Override
    public int compareTo(@NotNull Case1Cell o) {
        return Long.compare(this.ID, o.ID);
    }

    @Override
    public String toString() {
        return this.ID + "|" + String.format("%.2f", this.loanAmountSum);
    }
}
