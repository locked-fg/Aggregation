package de.locked.example;

import de.locked.aggregation.Container;
import de.locked.aggregation.Count;
import de.locked.aggregation.Id;
import de.locked.aggregation.Sum;
import de.locked.aggregation.SumAggregate;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class App {

    static Random r = new Random();

    public static void main(String[] args) {
        Container<Entity> container = new Container<>();
//        container.registerAggregate(new SumAggregate());

        long start = System.currentTimeMillis();
        for (int i = 0; i < 100_000; i++) {
            int a = r.nextInt(2) + 1;
            int b = r.nextInt(2) + 1;

            Entity aa = new Entity(a, b);
            container.aggregate(aa);
        }
        long stop = System.currentTimeMillis();
        System.out.println((stop - start) + "ms");

        for (Map.Entry<Container.Key, List<Container.Tuple>> entry : container.getResults().entrySet()) {
            System.out.println(entry.getKey());
            for (Container.Tuple t : entry.getValue()) {
                System.out.println("\t" + t);
            }
        }
    }

}

class Entity {

    @Id
    public int a = 0;
    @Id
    public long b = 1;

    @Count(alias = "count")
    public int cnt = 0;

    @Sum(alias = "mySum")
    public int cnt2 = 0;

    public Entity(int a, int b) {
        this.a = a;
        this.b = b;
        this.cnt2 = a + b;
    }
}
