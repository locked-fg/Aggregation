# Aggregation

## Motivation 
In the past I faced the problem that I had to aggregate several fields of (streamed) objects. But just aggregating over 
all objects would have been be too easy. What I actually needed was an object oriented version of 
```sql
SELECT field1, field2, field3, SUM(field_X) FROM objectlist GROUP BY field1, field2, field3
```
With ```objectlist``` being a list (or more often: a stream) of objects from classes like this:
```java
class Entity {
 // group by those fields
 int field1;
 String field2;
 boolean field3;
 
 // sum this field
 double field_x;
}
```
Holding all objects in memory and summing/grouping at the end wasn't an option in our cases where millions of 
entities are streamed through the application. And especially defining the aggregates and a pseudo primary key by 
hand is far from coder friendly and error prone as it involves overriding hashCode(), equals() which is usually 
very boilerplate.
This sometimes ended up in disgusting constructs like ```Map<Integer, Map<String, Map<Boolean, Double>>```.
Of course iterating through the result was no fun, too, as we always needed several nested loops.

So I wrote this aggregation container that works with simple classes and annotations.
The entities just need to be annotated and pushed into the container. The rest is done automatically. No need to 
override hashCode() or equals(), no extending classes, no nested/multi dimensional HashMaps.
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

    public static void main(String[] args) {
        // setup
        Random rand = new Random();
        Container<Entity> container = new Container<>();

        // do the aggregation
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10_000_000; i++) {
            container.aggregate(new Entity(rand.nextInt(2), rand.nextInt(2)));
        }
        long stop = System.currentTimeMillis();
        System.out.println((stop - start) + "ms");

        // show the result
        for (Container.Result result : container.getResults()) {
            System.out.println(result.toString()); // print the primary key(s)
            for (String fieldName : container.getAliases()) {
                System.out.println("\t" + fieldName + ": " + result.getDouble(fieldName));
            }
        }
    }
}

class Entity {

    @Id(order = 0)
    int a;
    @Id(order = 1)
    long b;

    @Sum(alias = "mySum")
    int cnt1 = 0;

    @Count(alias = "count")
    @Avg(alias = "myAverage")
    int cnt2 = 0;

    public Entity(int a, int b) {
        this.a = a;
        this.b = b;
        this.cnt1 = a + b;
        this.cnt2 = a + b;
    }
}
```

Output:
```
2005ms
Key: 2 1 
	mySum: 7495221.0
	count: 2498407.0
	myAverage: 3.0
Key: 1 2 
	mySum: 7501335.0
	count: 2500445.0
	myAverage: 3.0
Key: 1 1 
	mySum: 5005712.0
	count: 2502856.0
	myAverage: 2.0
Key: 2 2 
	mySum: 9993168.0
	count: 2498292.0
	myAverage: 4.0

```

## Runtime
On my machine, the above example creates and aggregates 10 000 000 entities in ~2000ms.


## Changelog
v2.0 Reduced complexity by delegates.
v1.0 First public release