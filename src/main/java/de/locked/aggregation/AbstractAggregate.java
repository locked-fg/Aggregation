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

public abstract class AbstractAggregate {

    /**
     * The annotation that belongs to this aggregation
     */
    private final Class annotation;

    public AbstractAggregate(Class clazz) {
        if (!clazz.isAnnotation()) {
            throw new IllegalArgumentException("class must ne an Annotation");
        }
        this.annotation = clazz;
    }

    public abstract AbstractAggregate getInstance();

    public void apply(Object o) {
        throw new UnsupportedOperationException();
    }

    public void apply(char v) {
        throw new UnsupportedOperationException();
    }

    public void apply(boolean v) {
        throw new UnsupportedOperationException();
    }

    public void apply(double v) {
        throw new UnsupportedOperationException();
    }

    public abstract double value();

    public Class getAnnotation() {
        return annotation;
    }

}
