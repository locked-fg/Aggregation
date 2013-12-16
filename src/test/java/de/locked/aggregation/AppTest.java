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

import java.util.List;
import static org.junit.Assert.*;
import org.junit.Test;

public class AppTest {

    @Test
    public void mixedInheritenceTest() {
        Container<EntityA> ca = new Container<>();
        ca.aggregate(new EntityA("a"));
        ca.aggregate(new EntityB("b"));
        boolean a = false, b = false;
        for (Container.Result result : ca.getResults()) {
            Object o = result.getKeys()[0];
            a |= o.equals("a");
            b |= o.equals("b");
            List<Container.Element> elements = result.getElements();
            assertEquals(1, elements.size());
        }
        assertTrue(a);
        assertTrue(b);
    }

    @Test
    public void inheritenceTest2() {
        Container<EntityB> ca = new Container<>();
        ca.aggregate(new EntityB("a"));
        ca.aggregate(new EntityB("a"));
        ca.aggregate(new EntityB("b"));

        for (Container.Result result : ca.getResults()) {
            if (result.getKeys()[0].equals("a")) {
                assertEquals(20, result.getDouble("i"), 0.0001);
                assertEquals(2, result.getInt("j"));
            }
            if (result.getKeys()[0].equals("b")) {
                assertEquals(10, result.getDouble("i"), 0.0001);
                assertEquals(1, result.getInt("j"));
            }
        }
    }
}

class EntityA {

    @Id
    public String id;

    @Sum(alias = "i")
    public int i = 10;

    public EntityA(String id) {
        this.id = id;
    }
}

class EntityB extends EntityA {

    @Count(alias = "j")
    public int j = 1;

    public EntityB(String id) {
        super(id);
    }

}
