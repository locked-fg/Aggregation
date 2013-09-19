package de.locked.aggregation;

public abstract class Aggregate {

    public abstract Aggregate get();

    public abstract void apply(int v);

    public abstract double value();
}
