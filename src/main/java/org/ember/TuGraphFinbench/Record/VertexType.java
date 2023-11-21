package org.ember.TuGraphFinbench.Record;

import lombok.ToString;

@ToString
public enum VertexType {
    Account,
    Loan,
    Person;

    public byte toByte() {
        switch (this) {
            case Account:
                return 0;
            case Loan:
                return 1;
            case Person:
                return 2;
            default:
                throw new RuntimeException("Invalid node type");
        }
    }

    public static VertexType fromByte(byte b) {
        switch (b) {
            case 0:
                return Account;
            case 1:
                return Loan;
            case 2:
                return Person;
            default:
                throw new RuntimeException("Invalid node type");
        }
    }
}
