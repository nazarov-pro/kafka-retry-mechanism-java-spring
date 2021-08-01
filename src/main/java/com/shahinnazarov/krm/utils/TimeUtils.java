package com.shahinnazarov.krm.utils;

import java.time.LocalDateTime;
import java.time.ZoneId;

public interface TimeUtils {
    LocalDateTime now();

    LocalDateTime now(ZoneId zoneId);

    Long epochMillis();

}
