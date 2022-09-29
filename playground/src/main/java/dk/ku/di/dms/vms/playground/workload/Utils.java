package dk.ku.di.dms.vms.playground.workload;

import java.util.Random;

public class Utils {

    private static Random generate = new Random();

    private static int init_rand = 1;
    private static int C_255;
    private static int C_1023;
    private static int C_8191;

    /*
     * return number uniformly distributed b/w min and max, inclusive
     */
    public static int randomNumber(int min, int max) {
        int next = generate.nextInt();
        int div = next % ((max - min) + 1);
        if (div < 0) {
            div = div * -1;
        }

        return min + div;
    }

    public static int nuRand(int A, int x, int y) {

        int C = 0;

        if(init_rand == 1) {
            C_255 = randomNumber(0, 255);
            C_1023 = randomNumber(0, 1023);
            C_8191 = randomNumber(0, 8191);
            init_rand = 0;
        }

        switch (A) {
            case 255:
                C = C_255;
                break;
            case 1023:
                C = C_1023;
                break;
            default: //case 8191:
                C = C_8191;
                // throw new RuntimeException("NURand: unexpected value (%d) of A used\n" + A);
        }

        return (((randomNumber(0, A) | randomNumber(x, y)) + C) % (y - x + 1)) + x;

    }

    public static String lastName(int num) {
        String name;
        String[] n = {"BAR", "OUGHT", "ABLE", "PRI", "PRES",
                        "ESE", "ANTI", "CALLY", "ATION", "EING"};

        name = n[num / 100];
        name = name + n[(num / 10) % 10];
        name = name + n[num % 10];

        return name;
    }

    public static String makeAlphaString(int x, int y) {
        StringBuilder str = null;
        String temp = "0123456789" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "abcdefghijklmnopqrstuvwxyz";
        char[] alphanum = temp.toCharArray();
        int arrmax = 61;  /* index of last array element */
        int i;
        int len;
        len = randomNumber(x, y);

        //      BEFORE
//        for (i = 0; i < len; i++) {
//            if (str != null) {
//                str = str + alphanum[randomNumber(0, arrmax)];
//            } else {
//                str = "" + alphanum[randomNumber(0, arrmax)];
//            }
//        }

        str = new StringBuilder("" + alphanum[randomNumber(0, arrmax)]);
        for (i = 1; i < len; i++) {
            str.append(alphanum[randomNumber(0, arrmax)]);
        }

        return str.toString();

    }

}
