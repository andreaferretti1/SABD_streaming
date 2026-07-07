package it.sabd.queries.query2.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

public class Top10Output {

    public long minIngestionTime;
    public List<Q2Output> top10 = new ArrayList<>();

    public Top10Output(){}

    public static Top10Output getTop10Output(Top10Accumulator acc){

        Top10Output output = new Top10Output();
        output.minIngestionTime = acc.ingestionTime;

        PriorityQueue<Q2Output> queueCopy = new PriorityQueue<>(acc.queue);

        while (!queueCopy.isEmpty()) {
            output.top10.add(queueCopy.poll());
        }

        Collections.reverse(output.top10);

        return output;

    }
}
