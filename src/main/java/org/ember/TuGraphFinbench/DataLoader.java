package org.ember.TuGraphFinbench;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.ember.TuGraphFinbench.Record.RawEdge;
import org.ember.TuGraphFinbench.Record.RawVertex;
import org.ember.TuGraphFinbench.Record.VertexType;

public class DataLoader {

    public static final String splitBy = "\\|";

    public static List<RawVertex> loadNodes() {
        String[] files = { "Account.csv", "Loan.csv", "Person.csv" };
        List<RawVertex> list = new ArrayList<>();
        for (String file : files) {
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                br.readLine(); // skip header
                String line = "";
                while ((line = br.readLine()) != null) {
                    String[] tokens = line.split(splitBy);
                    long rawID = Long.parseLong(tokens[0]);
                    long loanAmount = file.equals("Loan.csv") ? Long.parseLong(tokens[1]) : 0;
                    list.add(new RawVertex(VertexType.Person, rawID, loanAmount));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    static void updateAccountTransferAccount(List<RawEdge> list) {
        try (BufferedReader br = new BufferedReader(new FileReader("AccountTransferAccount.csv"))) {
            br.readLine(); // skip header
            String line = "";
            while ((line = br.readLine()) != null) {
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
        try (BufferedReader br = new BufferedReader(new FileReader("LoanDepositAccount.csv"))) {
            br.readLine(); // skip header
            String line = "";
            while ((line = br.readLine()) != null) {
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
        try (BufferedReader br = new BufferedReader(new FileReader("PersonApplyLoan.csv"))) {
            br.readLine(); // skip header
            String line = "";
            while ((line = br.readLine()) != null) {
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
        try (BufferedReader br = new BufferedReader(new FileReader("PersonGuaranteePerson.csv"))) {
            br.readLine(); // skip header
            String line = "";
            while ((line = br.readLine()) != null) {
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
        try (BufferedReader br = new BufferedReader(new FileReader("PersonOwnAccount.csv"))) {
            br.readLine(); // skip header
            String line = "";
            while ((line = br.readLine()) != null) {
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
}
