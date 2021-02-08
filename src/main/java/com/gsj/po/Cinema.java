package com.gsj.po;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
public class Cinema {
    private Long id;
    private String areaName;
    private String name;
    private String address;
    private BigDecimal lon;
    private BigDecimal lat;
}
