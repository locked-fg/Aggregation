package de.locked.example;

import de.locked.aggregation.Container;
import de.locked.aggregation.Count;
import de.locked.aggregation.Id;
import de.locked.aggregation.Sum;
import java.util.Random;

public class App {

    static Random r = new Random();

    public static void main(String[] args) {
        Container<Entity> container = new Container<>();
        // container.registerAggregate(new SumAggregate());

        long start = System.currentTimeMillis();
        for (int i = 0; i < 1_000_000; i++) {
            int a = r.nextInt(2) + 1;
            int b = r.nextInt(2) + 1;

            Entity aa = new Entity(a, b);
            container.aggregate(aa);
        }
        long stop = System.currentTimeMillis();
        System.out.println((stop - start) + "ms");

        for (Container.Result entry : container.getResults()) {
            System.out.println(entry.toString());
            for (Container.Element ac : entry.getElements()) {
                System.out.println("\t" + ac.getAlias() + ": " + ac.getValue());
            }
        }
    }
}

class Entity {

    @Id(order = 0)
    int a = 0;
    @Id(order = 1)
    long b = 1;

    @Count(alias = "count")
    int cnt = 0;

    @Sum(alias = "mySum")
    int cnt2 = 0;

    public Entity(int a, int b) {
        this.a = a;
        this.b = b;
        this.cnt2 = a + b;
    }
}
