package com.shahinnazarov.krm.container.models;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class KafkaSslConfigModel {
    private final String protocol;
    private final String trustStoreLocation;
    private final String trustStorePassword;
    private final Boolean enabled;

}
