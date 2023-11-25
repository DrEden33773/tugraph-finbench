package org.ember.TuGraphFinbench;

import com.antgroup.geaflow.example.util.ResultValidator;
import org.ember.TuGraphFinbench.Exec.Case1;
import org.ember.TuGraphFinbench.Exec.Case2;
import org.ember.TuGraphFinbench.Exec.Case3;
import org.ember.TuGraphFinbench.Exec.Case4;

public class AllCases {

    public static void main(String[] args) {
        final String RESULT_FILE_PATH = args.length == 2 ? args[1] : "./target/tmp/data/result/finbench";
        ResultValidator.cleanResult(RESULT_FILE_PATH);
        Case1.main(args);
        Case2.main(args);
        Case3.main(args);
        Case4.main(args);
    }
}
