package de.locked.aggregation;

public abstract class AbstractAggregate {

    public abstract AbstractAggregate getInstance();

    public void apply(Object o) {
    }

    public void apply(char v) {
    }

    public void apply(boolean v) {
    }

    public void apply(double v) {
    }

    public abstract double value();

    public abstract Class getAnnotation();

}
