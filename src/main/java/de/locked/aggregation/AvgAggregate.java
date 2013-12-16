/*
 * Copyright 2013 Franz.
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

/**
 *
 * @author Franz
 */
public class AvgAggregate extends AbstractAggregate {

    private int i = 0;
    private double sum = 0;

    public AvgAggregate() {
        super(Avg.class);
    }

    @Override
    public void apply(double v) {
        i++;
        sum += v;
    }

    @Override
    public double getDouble() {
        return sum / i;
    }

    @Override
    public AbstractAggregate getInstance() {
        return new AvgAggregate();
    }
}
