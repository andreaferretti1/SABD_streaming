package it.sabd.queries.query1.utils;

public class Q1ResultWithTime extends Q1Result{

    public long startWindow;
    public long endWindow;
    public String airline;

    public Q1ResultWithTime(){
        super();
    }

    public Q1ResultWithTime(long startWindow, long endWindow, String airline, Q1Result result){
        super(result);
        this.startWindow = startWindow;
        this.endWindow = endWindow;
        this.airline = airline;
    }
}
