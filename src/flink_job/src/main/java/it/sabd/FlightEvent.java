package it.sabd;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FlightEvent implements Serializable {

    @JsonIgnore
    public long ingestionTime;

    public String OP_UNIQUE_CARRIER;
    public int ORIGIN_AIRPORT_ID;
    public int DEST_AIRPORT_ID;
    public double CANCELLED;
    public double DIVERTED;
    public Double DEP_DELAY;

    public FlightEvent() {}

    public static FlightEvent generateDummyFlightEvent(){
        FlightEvent event = new FlightEvent();

        // Imposto i campi in modo che gli stream filtrino l'evento dummy
        event.OP_UNIQUE_CARRIER = "dummy_carrier";
        event.CANCELLED = 1.0;
        event.DIVERTED = 1.0;

        event.ORIGIN_AIRPORT_ID = 0;
        event.DEST_AIRPORT_ID = 0;
        event.DEP_DELAY = 0.0;

        return event;
    }
}
