package de.locked.example;

import de.locked.aggregation.Avg;
import de.locked.aggregation.Container;
import de.locked.aggregation.Count;
import de.locked.aggregation.Id;
import de.locked.aggregation.Sum;
import java.util.Random;

public class App {

    public static void main(String[] args) {
        // setup
        Random rand = new Random();
        Container<Entity> container = new Container<>();

        // do the aggregation
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10_000_000; i++) {
            container.aggregate(new Entity(rand.nextInt(2), rand.nextInt(2)));
        }
        long stop = System.currentTimeMillis();
        System.out.println((stop - start) + "ms");

        // show the result
        for (Container.Result result : container.getResults()) {
            System.out.println(result.toString()); // print the primary key(s)
            for (String fieldName : container.getAliases()) {
                System.out.println("\t" + fieldName + ": " + result.getDouble(fieldName));
            }
        }
    }
}

class Entity {

    @Id(order = 0)
    public int a;
    @Id(order = 1)
    public long b;

    @Sum(alias = "mySum")
    public int cnt1 = 0;

    @Count(alias = "count")
    @Avg(alias = "myAverage")
    public int cnt2 = 0;

    public Entity(int a, int b) {
        this.a = a;
        this.b = b;
        this.cnt1 = a + b;
        this.cnt2 = a + b;
    }
}
