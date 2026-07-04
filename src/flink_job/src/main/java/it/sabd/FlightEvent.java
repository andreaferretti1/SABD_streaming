package it.sabd;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FlightEvent implements Serializable {

    public String OP_UNIQUE_CARRIER;
    public int ORIGIN_AIRPORT_ID;
    public int DEST_AIRPORT_ID;
    public double CANCELLED;
    public double DIVERTED;
    public Double DEP_DELAY;

    public FlightEvent() {}
}
