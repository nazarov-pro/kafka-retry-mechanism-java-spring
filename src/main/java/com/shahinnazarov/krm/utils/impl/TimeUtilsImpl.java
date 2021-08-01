package com.shahinnazarov.krm.utils.impl;

import com.shahinnazarov.krm.utils.Constants;
import com.shahinnazarov.krm.utils.TimeUtils;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import org.springframework.stereotype.Component;

@Component
public class TimeUtilsImpl implements TimeUtils {

    @Override
    public LocalDateTime now() {
        return LocalDateTime.now(ZoneId.of(Constants.DEFAULT_TIMEZONE));
    }

    @Override
    public LocalDateTime now(ZoneId zoneId) {
        return LocalDateTime.now(zoneId);
    }

    @Override
    public Long epochMillis() {
        return Instant.now().toEpochMilli();
    }
}
