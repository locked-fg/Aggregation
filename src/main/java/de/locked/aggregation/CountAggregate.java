package de.locked.aggregation;

public class CountAggregate extends Aggregate {

    int i = 0;

    @Override
    public void apply(int v) {
        i++;
    }

    @Override
    public double value() {
        return i;
    }

    @Override
    public Aggregate get() {
        return new CountAggregate();
    }
}
