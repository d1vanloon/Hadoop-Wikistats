import java.util.Random;

public class ReduceTest {

    public static void reduce(int[] days) {
	int greatestSpikeA = 0;
        int greatestSpikeB = 0;
        int greatestSpikeMagnitude = 0;
        for (int i = 0; i < days.length; i++) {
            int lookBacks = 5;
            if (i <= lookBacks) {
                lookBacks = i;
            }
            int currentGreatestSpike = 0;
            int currentA = 0;
            int currentB = 0;
            for (int j = 0; j < lookBacks; j++) {
                if (days[i] - days[i - j] > currentGreatestSpike) {
                    currentGreatestSpike = days[i] - days[i - j];
                    currentA = i - j;
                    currentB = i;
                }
            }
            if (currentGreatestSpike > greatestSpikeMagnitude) {
                greatestSpikeMagnitude = currentGreatestSpike;
                greatestSpikeA = currentA;
                greatestSpikeB = currentB;
            }
        }
        System.out.println("The Greatest spike is a value of: " + greatestSpikeMagnitude);
        System.out.println("This spike is from day " + greatestSpikeA + " to day " + greatestSpikeB);
    }
    
    public static void main(String args[]) {
	System.out.println("begining test");
	Random random = new Random();
	int[] days = new int[60];
	for (int i = 0; i < days.length; i++) {
	    //System.out.println("Random number: " + i + " " + random.nextInt(1000));
	    days[i] = random.nextInt(1000);
	    System.out.println("Position: " + i + " " + days[i]);
	}
	reduce(days);
	/*	days[10] = 0;
	days[13] = 100000;
	reduce(days);*/
    }
}