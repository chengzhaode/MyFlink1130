package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * TODO
 *
 * @author Robin
 * @version 1.0
 * @date 2021/5/14 21:00
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class AdsClickLog {
    private Long userId;
    private Long adsId;
    private String province;
    private String city;
    private Long timestamp;
}

