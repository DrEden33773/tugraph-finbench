package org.ember.TuGraphFinbench.Cell;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.jetbrains.annotations.NotNull;

@Data
@AllArgsConstructor
public class Case2Cell implements Comparable<Case2Cell> {
    long ID;
    long ringCount;

    @Override
    public int compareTo(@NotNull Case2Cell o) {
        return Long.compare(this.ID, o.ID);
    }

    @Override
    public String toString() {
        return this.ID + "|" + this.ringCount;
    }
}
