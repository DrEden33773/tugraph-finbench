package org.ember.TuGraphFinbench.Cell;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.jetbrains.annotations.NotNull;

@Data
@AllArgsConstructor
public class Case3Cell implements Comparable<Case3Cell> {
    long ID;
    double inOutRatio;

    @Override
    public int compareTo(@NotNull Case3Cell o) {
        return Long.compare(this.ID, o.ID);
    }

    @Override
    public String toString() {
        return this.ID + "|" + String.format("%.2f", this.inOutRatio);
    }
}
