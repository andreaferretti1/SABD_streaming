package it.sabd.queries.query2.utils;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class EOSTrigger extends Trigger<Object, Window> {
    @Override
    public TriggerResult onElement(Object event, long l, Window window, TriggerContext triggerContext) {
        triggerContext.registerEventTimeTimer(Long.MAX_VALUE - 1);
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long l, Window window, TriggerContext triggerContext) {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, Window window, TriggerContext triggerContext) {
        if (time == Long.MAX_VALUE - 1) {
            return TriggerResult.FIRE_AND_PURGE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(Window window, TriggerContext triggerContext) {
        triggerContext.deleteEventTimeTimer(Long.MAX_VALUE - 1);
    }

}
