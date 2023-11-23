package org.ember.TuGraphFinbench.Record;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import org.apache.commons.lang3.tuple.MutablePair;

import java.util.List;

@Data
@AllArgsConstructor
@ToString
public class Case2Vertex {
    long ID;
    long ringCount;
    List<MutablePair<Long, Long>> prevAncestors;
}
