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

public class MaxAggregate extends AbstractAggregate {

    double max = Double.MIN_VALUE;

    public MaxAggregate() {
        super(Max.class);
    }

    @Override
    public void apply(double v) {
        max = Math.max(v, max);
    }

    @Override
    public AbstractAggregate getInstance() {
        return new MaxAggregate();
    }

    @Override
    public double value() {
        return max;
    }
}
