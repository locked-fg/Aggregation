package de.locked.aggregation;

import static java.lang.annotation.ElementType.FIELD;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

public class App {

    static Random r = new Random();

    public static void main(String[] args) {
        Container<Entity> container = new Container<>(Entity.class);

        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000_000; i++) {
            int a = r.nextInt(2);
            int b = r.nextInt(2);

            Entity aa = new Entity(a, b);
            container.put(aa);
        }
        long stop = System.currentTimeMillis();
        System.out.println((stop - start) + "ms");
        System.out.println(container.toString());
    }

}

class Entity {

    @Id
    int a = 0;
    @Id
    long b = 1;

    @Count(alias = "count")
    int cnt = 0;

    public Entity(int a, int b) {
        this.a = a;
        this.b = b;
    }
}
