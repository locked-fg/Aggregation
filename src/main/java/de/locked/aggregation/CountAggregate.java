package de.locked.aggregation;

public class CountAggregate extends AbstractAggregate {

    private Class annotation = Count.class;
    private int i = 0;

    @Override
    public void apply(double v) {
        i++;
    }

    @Override
    public void apply(boolean v) {
        i++;
    }

    @Override
    public void apply(char v) {
        i++;
    }

    @Override
    public void apply(Object o) {
        i++;
    }

    @Override
    public double value() {
        return i;
    }

    @Override
    public AbstractAggregate getInstance() {
        return new CountAggregate();
    }

    @Override
    public Class getAnnotation() {
        return annotation;
    }
}
