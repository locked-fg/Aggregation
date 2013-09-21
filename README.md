# Aggregation

## Motivation
In the past time I often faced the problem that I had to aggregate several fields of (streamed) objects.

Given a class 
```java
class Entitiy {
 // group by those fields
 int field1;
 String field2;
 boolean field3;
 
 // sum this field
 double field_x;
}
```

## Problem
What I actually needed was an object oriented version of 
```sql
SELECT field1, field2, field3, SUM(field_X) FROM objectlist GROUP BY field1, field2, field3
```
Holding all objects in memory and summing/grouping at the end wasn't an option in our cases where millions of entities are streamed through your application. And especially defining the aggregates and a pseudo primary key by hand is far from coder friendly and error prone as it involves overriding hashode, equals etc ...

So I prototyped this aggregation container that works with simple classes and annotations.
The entities just need to be annotated and pushed into the container. The rest is done automatically. No hashcode, no equals, no extending classes:
```java
class Entity {

    @Id
    int a = 0;
    @Id
    long b = 1;

    @Count(alias = "count")
    int cnt = 0;

    @Sum(alias = "mySum")
    int cnt2 = 0;
}
```

---

## Example
The full example (also found in the example package):

```java
public class App {

    static Random r = new Random();

    public static void main(String[] args) {
        Container<Entity> container = new Container<>();
        // container.registerAggregate(new SumAggregate());

        long start = System.currentTimeMillis();
        for (int i = 0; i < 10_000_000; i++) {
            int a = r.nextInt(2) + 1;
            int b = r.nextInt(2) + 1;

            Entity aa = new Entity(a, b);
            container.aggregate(aa);
        }
        long stop = System.currentTimeMillis();
        System.out.println((stop - start) + "ms");

        for (Container.Result entry : container.getResults()) {
            System.out.println(entry.toString());
            for (Container.Element ac : entry.getElements()) {
                System.out.println("\t" + ac.getAlias() + ": " + ac.getValue());
            }
        }
    }
}

class Entity {

    @Id(order = 0)
    int a = 0;
    @Id(order = 1)
    long b = 1;

    @Count(alias = "count")
    int cnt = 0;

    @Sum(alias = "mySum")
    int cnt2 = 0;

    public Entity(int a, int b) {
        this.a = a;
        this.b = b;
        this.cnt2 = a + b;
    }
}
```

Output:
```
1757ms
Key: 2 1 
	count: 2498433.0
	mySum: 7495299.0
Key: 1 2 
	count: 2500971.0
	mySum: 7502913.0
Key: 1 1 
	count: 2499335.0
	mySum: 4998670.0
Key: 2 2 
	count: 2501261.0
	mySum: 1.0005044E7
```

## Runtime
On my machine, the above example creates and aggregates 10 000 000 entities in ~1700ms.
