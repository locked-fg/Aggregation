package de.locked.aggregation;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class Container<T> {

    private static final Logger LOG = Logger.getLogger(Container.class.getName());

    private final List<Field> idFields;

    private final List<AbstractAggregate> aggregates;

    // precomputed Link from class fields to what we want to compute
    private final List<AggregationContainer> aggregationMapCache = new ArrayList<>();
    // aggregation from a primary key (Key) to the aggregation
    // this is what you actually want to iterate afterwards!
    private final Map<Key, List<AggregationContainer>> resultAggregation = new HashMap<>();

    private State currentState = new OpenState();

    public Container() {
        this.idFields = new ArrayList<>();
        this.aggregates = new ArrayList<>();

        // register Default Aggregates
        aggregates.add(new CountAggregate());
    }

    public HashMap<Key, List<Tuple>> getResults() {
        HashMap<Key, List<Tuple>> result = new HashMap<>();
        for (Map.Entry<Key, List<AggregationContainer>> entry : resultAggregation.entrySet()) {
            List<Tuple> list = new ArrayList<>();
            for (AggregationContainer container : entry.getValue()) {
                list.add(new Tuple(container.alias, container.agg.value()));
            }
            result.put(entry.getKey(), list);
        }
        return result;
    }

    // called from state
    private void doPrepare(Class clazz) {
        Field[] fields = clazz.getDeclaredFields();
        for (Field f : fields) {
            if (f.isAnnotationPresent(Id.class)) {
                idFields.add(f);
            }
            for (AbstractAggregate aggregate : aggregates) {
                Class annotationClass = aggregate.getAnnotation();
                if (f.isAnnotationPresent(annotationClass)) {
                    Annotation annotation = f.getAnnotation(annotationClass);
                    String alias = getAlias(annotation);
                    AggregationContainer tuple = new AggregationContainer(aggregate, alias, f);
                    aggregationMapCache.add(tuple);
                }
            }
        }
    }

    private String getAlias(Annotation a) {
        try {
            String alias = a.getClass().getName();
            Method method = a.getClass().getMethod("alias");
            if (method != null) {
                Object returnString = method.invoke(a);
                if (returnString != null && returnString instanceof String) {
                    alias = (String) returnString;
                }
            }
            return alias;
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException |
                InvocationTargetException ex) {
            throw new IllegalArgumentException("Couldn't extract 'alias' from annotation " + a.getClass(), ex);
        }
    }

    private List<AggregationContainer> getMap() {
        List<AggregationContainer> m = new ArrayList<>(aggregationMapCache.size());
        for (AggregationContainer e : aggregationMapCache) {
            m.add(e.get());
        }
        return m;
    }

    public void aggregate(T object) {
        currentState.aggregate(object);
    }

    // called from state
    private void doAggregate(T object) {
        try {
            // build key
            Object[] k = new Object[idFields.size()];
            for (int i = 0; i < idFields.size(); i++) {
                Field f = idFields.get(i);
                k[i] = f.get(object);
            }
            Key key = new Key(k);

            // new Primary Key?
            List<AggregationContainer> tupleList = resultAggregation.get(key);
            if (tupleList == null) {
                tupleList = getMap();
                resultAggregation.put(key, tupleList);
            }

            // do the aggregation(s)
            for (AggregationContainer tuple : tupleList) {
                Field f = tuple.field;
                Class type = tuple.type;
                AbstractAggregate agg = tuple.agg;

                if (type.equals(int.class)) {
                    agg.apply(f.getInt(object));
                } else if (type.equals(byte.class)) {
                    agg.apply(f.getByte(object));
                } else if (type.equals(float.class)) {
                    agg.apply(f.getFloat(object));
                } else if (type.equals(double.class)) {
                    agg.apply(f.getDouble(object));
                } else if (type.equals(long.class)) {
                    agg.apply(f.getLong(object));
                } else if (type.equals(char.class)) {
                    agg.apply(f.getChar(object));
                } else if (type.equals(short.class)) {
                    agg.apply(f.getShort(object));
                } else if (type.equals(boolean.class)) {
                    agg.apply(f.getBoolean(object));
                } else {
                    agg.apply(f.get(object));
                }
            }
        } catch (IllegalArgumentException | IllegalAccessException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public void registerAggregate(AbstractAggregate agg) {
        currentState.register(agg);
    }

    private void doRegisterAggregate(AbstractAggregate agg) {
        aggregates.add(agg);
    }

    @Override
    public String toString() {
        String s = "";
        for (Map.Entry<Key, List<AggregationContainer>> e : resultAggregation.entrySet()) {
            Key key = e.getKey();
            s += key.toString() + ": ";

            List<AggregationContainer> tuples = e.getValue();
            for (AggregationContainer tuple : tuples) {
                s += "\n\t" + tuple.alias + ": " + tuple.agg.value();
            }
            s += "\n";
        }
        return s;
    }

    public static class Key {

        private final Object[] keys;

        public Key(Object[] keys) {
            this.keys = keys;
        }

        /**
         * @return returns a COPY of the keys array
         */
        public Object[] getKeys() {
            return keys.clone();
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

    public static class Tuple {

        private final String alias;
        private final double value;

        private Tuple(String alias, double value) {
            this.alias = alias;
            this.value = value;
        }

        public String getAlias() {
            return alias;
        }

        public double getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "Tuple{" + "alias=" + alias + ", value=" + value + '}';
        }
    }

    private class AggregationContainer {

        private final String alias;
        private final AbstractAggregate agg;
        private final Class type;
        private final Field field;

        public AggregationContainer(AbstractAggregate agg, String alias, Field field) {
            this.agg = agg;
            this.type = field.getType();
            this.alias = alias;
            this.field = field;
        }

        public AggregationContainer get() {
            return new AggregationContainer(agg.getInstance(), alias, field);
        }
    }

    private interface State<T> {

        void register(AbstractAggregate agg);

        void aggregate(T o);
    }

    private class OpenState implements State<T> {

        @Override
        public void register(AbstractAggregate agg) {
            doRegisterAggregate(agg);
        }

        @Override
        public void aggregate(T o) {
            doPrepare(o.getClass());
            currentState = new AggregateState();
            currentState.aggregate(o);
        }
    }

    private class AggregateState implements State<T> {

        @Override
        public void register(AbstractAggregate agg) {
            throw new IllegalStateException("You can only register new functions before doing the first aggregate.");
        }

        @Override
        public void aggregate(T o) {
            doAggregate(o);
        }
    }

}
