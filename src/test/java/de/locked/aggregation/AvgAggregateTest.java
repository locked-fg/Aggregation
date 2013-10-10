/*
 * Copyright 2013 Dr. Franz Graf <info@Locked.de>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.locked.aggregation;

import java.util.Random;
import org.junit.Test;
import static org.junit.Assert.*;

public class AvgAggregateTest {

    @Test
    public void testApplication() {
        Container<Entity> container = new Container<>();
        container.aggregate(new Entity(1, 1));
        container.aggregate(new Entity(1, 3));
        container.aggregate(new Entity(2, 0));
        container.aggregate(new Entity(2, 10));
        container.aggregate(new Entity(3, 10));
        container.aggregate(new Entity(4, 10));
        container.aggregate(new Entity(4, -10));

        for (Container.Result entry : container.getResults()) {
            Object key = entry.getKeys()[0];
            double value = entry.getAggregate("value").getDouble();
            if (key.equals(1)) {
                assertEquals(2, value, 0.000d);
            } else if (key.equals(2)) {
                assertEquals(5, value, 0.000d);
            } else if (key.equals(3)) {
                assertEquals(10, value, 0.000d);
            } else if (key.equals(4)) {
                assertEquals(0, value, 0.000d);
            }
        }
    }

    class Entity {

        @Id(order = 0)
        int key;

        @Avg(alias = "value")
        int value = 0;

        public Entity(int a, int b) {
            this.key = a;
            this.value = b;
        }
    }
}
