/*
 * Copyright 2013 Dr. Franz Graf.
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

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

public class Container<T> {

    private static final Logger LOG = Logger.getLogger(Container.class.getName());

    // fields that cat as primary keys
    private final List<Field> idFields;

    // aggregate objects that were registered
    private final List<AbstractAggregate> aggregates;

    // precomputed Link from class fields to what we want to compute
    private final List<Element> aggregationMapCache = new ArrayList<>();

    // aggregation from a primary key (Key) to the aggregation
    // this is what you actually want to iterate afterwards!
    private final Map<Result, Result> resultAggregation;

    // current state of the container
    private State currentState = new OpenState();

    public Container() {
        this.resultAggregation = new HashMap<>();
        this.idFields = new ArrayList<>();
        this.aggregates = new ArrayList<>();

        // register Default Aggregates
        aggregates.add(new CountAggregate());
        aggregates.add(new SumAggregate());
        aggregates.add(new AvgAggregate());
        aggregates.add(new MinAggregate());
        aggregates.add(new MaxAggregate());
    }

    /**
     * Obtain the result of the operation.
     *
     * @return Map from key to list of results
     */
    public Collection<Result> getResults() {
        return resultAggregation.keySet();
    }

    /**
     * Checked call from the state machine at the first call to aggregate.
     *
     * @param clazz the class of the aggregate object.
     */
    private void doPrepare(Class clazz) {
        Field[] fields = clazz.getDeclaredFields();
        for (Field f : fields) {
            if (f.isAnnotationPresent(Id.class)) {
                f.setAccessible(true);
                idFields.add(f);
            }
            for (AbstractAggregate aggregate : aggregates) {
                Class annotationClass = aggregate.getAnnotation();
                if (f.isAnnotationPresent(annotationClass)) {
                    Annotation annotation = f.getAnnotation(annotationClass);
                    String alias = getAlias(annotation);
                    Element tuple = new Element(aggregate, alias, f);
                    aggregationMapCache.add(tuple);
                }
            }
        }

        // sort the id fields by the order
        Collections.sort(idFields, new Comparator<Field>() {

            @Override
            public int compare(Field o1, Field o2) {
                int x = o1.getAnnotation(Id.class).order();
                int y = o2.getAnnotation(Id.class).order();
                return Integer.compare(x, y);
            }
        });
    }

    /**
     * Get and return the "alias" value from the annotation.
     *
     * If no such annotation is present, the class name is returned as a default.
     *
     * @param annotation The Annotation to get the value from
     * @return the 'value' or the classname if no value is found (which shouldn't be)
     */
    private String getAlias(Annotation annotation) {
        try {
            String alias = annotation.getClass().getName();
            Method method = annotation.getClass().getMethod("alias");
            if (method != null) {
                Object returnString = method.invoke(annotation);
                if (returnString != null && returnString instanceof String) {
                    alias = (String) returnString;
                }
            }
            return alias;
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException |
                InvocationTargetException ex) {
            throw new IllegalArgumentException("Couldn't extract 'alias' from annotation " + annotation.getClass(), ex);
        }
    }

    /**
     * Initializes a copy of the aggregations.
     *
     * @return List of aggregation containers
     */
    private List<Element> getCopy() {
        List<Element> m = new ArrayList<>(aggregationMapCache.size());
        for (Element e : aggregationMapCache) {
            m.add(e.getInstance());
        }
        return m;
    }

    /**
     * Add this object to the aggregation container.
     *
     * @param object
     */
    public void aggregate(T object) {
        currentState.aggregate(object);
    }

    /**
     * checked aggregate call from the state machine
     *
     * @param object The object that should be aggregated
     */
    private void doAggregate(T object) {
        try {
            Result key = getKeyFor(object);

            // do the aggregation(s)
            for (Element tuple : key.elements) {
                Field f = tuple.field;
                Class type = f.getType();
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

    /**
     * Register Aggregate to the container
     *
     * @param agg The aggregate object to the Container if this object is not already registered.
     */
    public void registerAggregate(AbstractAggregate agg) {
        currentState.register(agg);
    }

    /**
     * checked register call from the state machine
     *
     * @param agg the Aggregate that should be registered into the container
     */
    private void doRegisterAggregate(AbstractAggregate agg) {
        if (!aggregates.contains(agg)) {
            aggregates.add(agg);
        }
    }

    @Override
    public String toString() {
        String s = "";
        for (Result key : resultAggregation.keySet()) {
            s += key.toString() + ": ";
            for (Element tuple : key.elements) {
                s += "\n\t" + tuple.alias + ": " + tuple.agg.value();
            }
            s += "\n";
        }
        return s;
    }

    /**
     * Get the according key from the map.
     *
     * @param requestKey
     * @return
     */
    private Result getKeyFor(T object) throws IllegalArgumentException, IllegalAccessException {
        Result requestKey = buildRequestKey(object);
        Result key = resultAggregation.get(requestKey);
        if (key == null) {
            requestKey.init(getCopy());
            resultAggregation.put(requestKey, requestKey);
            key = requestKey;
        }
        return key;
    }

    // 35% of exec time are burnt in this method.
    private Result buildRequestKey(T object) throws IllegalAccessException, IllegalArgumentException {
        Object[] k = new Object[idFields.size()];
        for (int i = 0; i < idFields.size(); i++) {
            Field f = idFields.get(i);
            k[i] = f.get(object);
        }
        return new Result(k);
    }

    public static class Result {

        private final Object[] keys;
        private List<Element> elements;

        /**
         * Create a new Key Object from the given object array as primary keys.
         *
         * @param keys the Objects representing the primary keys.
         */
        private Result(Object[] keys) {
            this.keys = keys;
        }

        /**
         * @return List of aggregate elements
         */
        public List<Element> getElements() {
            return elements;
        }

        /**
         * @return map pointing from the alias to the element
         */
        public Map<String, Element> getElementsMap() {
            Map<String, Element> result = new HashMap<>();
            for (Element element : elements) {
                result.put(element.getAlias(), element);
            }
            return result;
        }

        /**
         * Returns the value for the given alias.
         *
         * @param alias (sum, avg, min, ...)
         * @return the according value or Double.NaN if no such element was found
         */
        public double getElement(String alias) {
            for (Element element : elements) {
                if (element.getAlias().equals(alias)) {
                    return element.getValue();
                }
            }
            return Double.NaN;
        }

        /**
         * @return returns a COPY of the keys array.
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
            int hash = 59 + Arrays.deepHashCode(this.keys);
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
            final Result other = (Result) obj;
            if (!Arrays.deepEquals(this.keys, other.keys)) {
                return false;
            }
            return true;
        }

        private void init(List<Element> list) {
            this.elements = Collections.unmodifiableList(list);
        }
    }

    /**
     * Result class holding the tuple for annotation alias : aggregation value.
     */
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

    /**
     * The Element class wraps a part of an aggregation result.
     *
     * The element itself is identified by the alias, the field to which the alias was assigned and the
     * (Abstract)Aggregate object itself. One aggragtion container stores multiple Elements - one for each field.
     * 
     * The class might get renamed to a more appropriate name in the future.
     */
    public static class Element {

        private final String alias;
        private final AbstractAggregate agg;
        private final Field field;

        public Element(AbstractAggregate agg, String alias, Field field) {
            this.agg = agg;
            this.alias = alias;
            this.field = field;
            field.setAccessible(true);
        }

        public Element getInstance() {
            return new Element(agg.getInstance(), alias, field);
        }

        public String getAlias() {
            return alias;
        }

        public double getValue() {
            return agg.value();
        }
    }

    /**
     * State machine for checking the currect call order.
     *
     * THis avoids that register is called AFTER the first aggregate. Which would result in an invalid state.
     */
    private interface State<T> {

        void register(AbstractAggregate agg);

        void aggregate(T o);
    }

    /**
     * State before the first call to aggregate.
     */
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

    /**
     * Aggregate State.
     *
     * No more register calls are allowed
     */
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
