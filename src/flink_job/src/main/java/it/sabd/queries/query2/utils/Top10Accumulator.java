package it.sabd.queries.query2.utils;

import java.util.Comparator;
import java.util.PriorityQueue;

public class Top10Accumulator {

    public long ingestionTime = Long.MAX_VALUE;
    public PriorityQueue<Q2Output> queue = new PriorityQueue<Q2Output>(10, Comparator.comparingInt(output -> output.totSignificantDelayedFLights));

    public Top10Accumulator(){}

    public void addEvent(Q2Output output){

        this.ingestionTime = Math.min(this.ingestionTime, output.minIngestionTime);

        this.addToQueue(output);
    }

    public void merge(Top10Accumulator acc){

        this.ingestionTime = Math.min(this.ingestionTime, acc.ingestionTime);

        for (Q2Output output : acc.queue) {
            this.addToQueue(output);
        }


    }

    private void addToQueue(Q2Output output){

        if(this.queue.size() < 10){
            this.queue.offer(output);
        } else {
            Q2Output min = this.queue.peek();

            if(output.totSignificantDelayedFLights > min.totSignificantDelayedFLights){
                this.queue.poll();
                this.queue.offer(output);
            }
        }
    }
}
