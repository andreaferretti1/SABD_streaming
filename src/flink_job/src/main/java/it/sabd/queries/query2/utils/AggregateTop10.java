package it.sabd.queries.query2.utils;

import org.apache.flink.api.common.functions.AggregateFunction;


import java.util.*;

public class AggregateTop10 implements AggregateFunction<Q2Output, PriorityQueue<Q2Output>, List<Q2Output>> {

    @Override
    public PriorityQueue<Q2Output> createAccumulator() {
        return new PriorityQueue<Q2Output>(10, Comparator.comparingInt(output -> output.totSignificantDelayedFLights));
    }

    @Override
    public PriorityQueue<Q2Output> add(Q2Output output, PriorityQueue<Q2Output> queue) {
       if(queue.size() < 10){
           queue.offer(output);
       } else {
           Q2Output min = queue.peek();

           if(output.totSignificantDelayedFLights > min.totSignificantDelayedFLights){
               queue.poll();
               queue.offer(output);
           }
       }

       return queue;
    }

    @Override
    public List<Q2Output> getResult(PriorityQueue<Q2Output> queue) {
        List<Q2Output> top10 = new ArrayList<Q2Output>();
        PriorityQueue<Q2Output> queueCopy = new PriorityQueue<>(queue);

        while (!queueCopy.isEmpty()) {
            top10.add(queueCopy.poll());
        }

        Collections.reverse(top10);
        return top10;
    }

    @Override
    public PriorityQueue<Q2Output> merge(PriorityQueue<Q2Output> queue1, PriorityQueue<Q2Output> queue2) {

        for (Q2Output output : queue2) {
            this.add(output, queue1);
        }

        return queue1;
    }
}
