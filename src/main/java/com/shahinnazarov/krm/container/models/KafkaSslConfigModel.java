package com.shahinnazarov.krm.container.models;

import lombok.Data;

@Data
public class KafkaSslConfigModel {
    private String protocol;
    private String trustStoreLocation;
    private String trustStorePassword;
    private Boolean enabled = false;

}
