package com.whirly.recipes.configcenter;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @program: curator-example
 * @description:
 * @author: 赖键锋
 * @create: 2019-01-22 17:07
 **/
@AllArgsConstructor
@Data
public class MysqlConfig {
    private String url;
    private String driver;
    private String username;
    private String password;
}
