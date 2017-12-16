package com.hadoop.matrix.job;


import com.hadoop.matrix.step1.MR1;
import com.hadoop.matrix.step2.MR2;

public class JobRunner {
    public static void main(String[] args) {
        int status1 = -1;
        int status2 = -1;
        status1 = new MR1().run();
        if (status1 == 1) {
            System.out.println("Step 1 Success....");
            status2 = new MR2().run();
        } else {
            System.out.println("Step 1 Fail....");
        }
        if (status2 == 1) {
            System.out.println("Step 2 Success....Job Complete...");
        } else {
            System.out.println("Step 2 Fail....");
        }

    }
}
