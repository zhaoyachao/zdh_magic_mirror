package com.zyc.ship.disruptor;

import com.lmax.disruptor.EventFactory;

public class ShipEventFactory implements EventFactory {
    @Override
    public Object newInstance() {
        return new ShipEvent();
    }
}
