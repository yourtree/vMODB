package dk.ku.di.dms.vms.utils;

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
        int value = min + div;

        return value;
    }

    public static float randomUniformDouble(){
        // return a + sb_rand_uniform_double() * (b - a + 1);
        return generate.nextFloat();
    }

    /* Return a uniformly distributed pseudo-random double in the [0, 1) interval */


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

}
