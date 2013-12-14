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

import java.util.Collection;
import java.util.Random;
import org.junit.Test;
import static org.junit.Assert.*;

public class DistinctAggregateTest {

    @Test
    public void testApplication() {
        Container<Entity> container = new Container<>();
        container.aggregate(new Entity(1, "a"));
        container.aggregate(new Entity(2, "a"));
        container.aggregate(new Entity(2, "b"));
        container.aggregate(new Entity(3, "a"));
        container.aggregate(new Entity(3, "a"));
        container.aggregate(new Entity(3, "c"));

        for (Container.Result entry : container.getResults()) {
            Object key = entry.getKeys()[0];
            Collection values = entry.getCollection("value");
            if (key.equals(1)) {
                assertEquals(1, values.size());
                assertTrue(values.contains("a"));
            } else if (key.equals(2)) {
                assertEquals(2, values.size());
                assertTrue(values.contains("a"));
                assertTrue(values.contains("b"));
            } else if (key.equals(3)) {
                assertEquals(2, values.size());
                assertTrue(values.contains("a"));
                assertTrue(values.contains("c"));
            }
        }
    }

    class Entity {

        @Id(order = 0)
        int key;

        @Distinct(alias = "value")
        String value;

        public Entity(int a, String b) {
            this.key = a;
            this.value = b;
        }
    }
}
