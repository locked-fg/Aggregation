package de.locked.aggregation;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class Container<T> {

    private static final Logger LOG = Logger.getLogger(Container.class.getName());

    // precomputed Link from class fields to what we want to compute
    private final Map<Field, Tuple> baseAggregationMap = new HashMap<>();
    // aggregation from a primary key 
    private final Map<Key, Map<Field, Tuple>> map = new HashMap<>();

    private final List<Field> idFields;

    private class Key {

        private final Object[] keys;

        public Key(Object[] keys) {
            this.keys = keys;
        }

        @Override
        public String toString() {
            String s = "Key: ";
            for (Object o : keys) {
                s += o.toString() + " ";
            }
            return s;
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 41 * hash + Arrays.deepHashCode(this.keys);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final Key other = (Key) obj;
            if (!Arrays.deepEquals(this.keys, other.keys)) {
                return false;
            }
            return true;
        }
    }

    private class Tuple {

        private final String alias;
        private final Aggregate agg;
        private final Class type;

        public Tuple(Class<? extends Aggregate> clazz, String alias, Class type) {
            try {
                this.agg = clazz.newInstance();
                this.alias = alias;
                this.type = type;
            } catch (InstantiationException | IllegalAccessException ex) {
                throw new IllegalArgumentException(ex);
            }
        }

        public Tuple get() {
            return new Tuple(agg.getClass(), alias, type);
        }
    }

    public Container(Class<T> clazz) {
        this.idFields = new ArrayList<>();

        Field[] fields = clazz.getDeclaredFields();
        for (Field f : fields) {
            if (f.isAnnotationPresent(Id.class)) {
                idFields.add(f);
            }
            if (f.isAnnotationPresent(Count.class)) {
                String alias = f.getAnnotation(Count.class).alias();
                Class type = f.getType();
                Tuple tuple = new Tuple(CountAggregate.class, alias, type);
                baseAggregationMap.put(f, tuple);
            }
        }
    }

    private Map<Field, Tuple> getMap() {
        Map<Field, Tuple> m = new HashMap<>(baseAggregationMap.size());
        for (Map.Entry<Field, Tuple> e : baseAggregationMap.entrySet()) {
            m.put(e.getKey(), e.getValue().get());
        }
        return Collections.unmodifiableMap(m);
    }

    public void put(T object) {
        try {
            Object[] k = new Object[idFields.size()];
            for (int i = 0; i < idFields.size(); i++) {
                Field f = idFields.get(i);
                k[i] = f.get(object);
            }
            Key key = new Key(k);

            Map<Field, Tuple> tupleMap = map.get(key);
            if (tupleMap == null) {
                tupleMap = getMap();
                map.put(key, tupleMap);
            }

            for (Map.Entry<Field, Tuple> e : tupleMap.entrySet()) {
                Field f = e.getKey();
                Tuple tuple = e.getValue();
                if (tuple.type.equals(int.class)) {
                    int v = (int) f.get(object);
                    tuple.agg.apply(v);
                } else {
                    throw new IllegalArgumentException("uncool " + tuple.type);
                }
            }
        } catch (IllegalArgumentException | IllegalAccessException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public String toString() {
        String s = "";
        for (Map.Entry<Key, Map<Field, Tuple>> e : map.entrySet()) {
            Key key = e.getKey();
            s += key.toString() + ": ";

            Map<Field, Tuple> tuples = e.getValue();
            for (Tuple tuple : tuples.values()) {
                s += "\n\t" + tuple.alias + ": " + tuple.agg.value();
            }
            s += "\n";
        }
        return s;
    }
}
