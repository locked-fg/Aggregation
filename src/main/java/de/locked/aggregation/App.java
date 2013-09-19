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
import java.util.logging.Level;
import java.util.logging.Logger;

public class App {

    public static void main(String[] args) {
        Entity a = new Entity(1, 0);
        Entity b = new Entity(1, 0);
        Entity c = new Entity(1, 0);
        Entity d = new Entity(0, 0);

        Container<Entity> container = new Container<>(Entity.class);
        container.put(a);
        container.put(b);
        container.put(c);
        container.put(d);

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


