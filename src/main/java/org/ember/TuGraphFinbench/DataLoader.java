package org.ember.TuGraphFinbench;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.ember.TuGraphFinbench.Record.RawEdge;
import org.ember.TuGraphFinbench.Record.RawVertex;
import org.ember.TuGraphFinbench.Record.VertexType;

import com.google.common.io.Resources;

public class DataLoader {

    public static final String splitBy = "\\|";

    public static List<RawVertex> loadVertices() {
        String[] files = { "Account.csv", "Loan.csv", "Person.csv" };
        List<RawVertex> list = new ArrayList<>();
        for (String file : files) {
            try {
                List<String> lines = Resources.readLines(Resources.getResource(file), Charset.defaultCharset());
                boolean isHeader = true;
                for (String line : lines) {
                    if (isHeader) {
                        isHeader = false;
                        continue;
                    }
                    String[] tokens = line.split(splitBy);
                    long rawID = Long.parseLong(tokens[0]);
                    double loanAmount = file.equals("Loan.csv") ? Double.parseDouble(tokens[1]) : 0;
                    list.add(new RawVertex(VertexType.Person, rawID, loanAmount));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    static void updateAccountTransferAccount(List<RawEdge> list) {
        try {
            List<String> lines = Resources.readLines(Resources.getResource("AccountTransferAccount.csv"),
                    Charset.defaultCharset());
            boolean isHeader = true;
            for (String line : lines) {
                if (isHeader) {
                    isHeader = false;
                    continue;
                }
                String[] tokens = line.split(splitBy);
                long fromID = Long.parseLong(tokens[0]);
                long toID = Long.parseLong(tokens[1]);
                double amount = Double.parseDouble(tokens[2]);
                list.add(new RawEdge(fromID, VertexType.Account, toID, VertexType.Account, amount));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void updateLoanDepositAccount(List<RawEdge> list) {
        try {
            List<String> lines = Resources.readLines(Resources.getResource("LoanDepositAccount.csv"),
                    Charset.defaultCharset());
            boolean isHeader = true;
            for (String line : lines) {
                if (isHeader) {
                    isHeader = false;
                    continue;
                }
                String[] tokens = line.split(splitBy);
                long loanID = Long.parseLong(tokens[0]);
                long accountID = Long.parseLong(tokens[1]);
                double amount = Double.parseDouble(tokens[2]);
                list.add(new RawEdge(loanID, VertexType.Loan, accountID, VertexType.Account, amount));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void updatePersonApplyLoan(List<RawEdge> list) {
        try {
            List<String> lines = Resources.readLines(Resources.getResource("PersonApplyLoan.csv"),
                    Charset.defaultCharset());
            boolean isHeader = true;
            for (String line : lines) {
                if (isHeader) {
                    isHeader = false;
                    continue;
                }
                String[] tokens = line.split(splitBy);
                long personID = Long.parseLong(tokens[0]);
                long loanID = Long.parseLong(tokens[1]);
                list.add(new RawEdge(personID, VertexType.Person, loanID, VertexType.Loan, 0));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void updatePersonGuaranteePerson(List<RawEdge> list) {
        try {
            List<String> lines = Resources.readLines(Resources.getResource("PersonGuaranteePerson.csv"),
                    Charset.defaultCharset());
            boolean isHeader = true;
            for (String line : lines) {
                if (isHeader) {
                    isHeader = false;
                    continue;
                }
                String[] tokens = line.split(splitBy);
                long fromID = Long.parseLong(tokens[0]);
                long toID = Long.parseLong(tokens[1]);
                list.add(new RawEdge(fromID, VertexType.Person, toID, VertexType.Person, 0));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void updatePersonOwnAccount(List<RawEdge> list) {
        try {
            List<String> lines = Resources.readLines(Resources.getResource("PersonOwnAccount.csv"),
                    Charset.defaultCharset());
            boolean isHeader = true;
            for (String line : lines) {
                if (isHeader) {
                    isHeader = false;
                    continue;
                }
                String[] tokens = line.split(splitBy);
                long personID = Long.parseLong(tokens[0]);
                long accountID = Long.parseLong(tokens[1]);
                list.add(new RawEdge(personID, VertexType.Person, accountID, VertexType.Account, 0));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static List<RawEdge> loadEdges() {
        List<RawEdge> list = new ArrayList<>();
        updateAccountTransferAccount(list);
        updateLoanDepositAccount(list);
        updatePersonApplyLoan(list);
        updatePersonGuaranteePerson(list);
        updatePersonOwnAccount(list);
        return list;
    }

    public static List<RawVertex> rawVertices = loadVertices();

    public static List<RawEdge> rawEdges = loadEdges();
}
