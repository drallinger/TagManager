package com.drallinger.tagmanager;

import java.sql.ResultSet;
import java.sql.SQLException;

@FunctionalInterface
public interface CreateObjectFunction<T> {
    T execute(ResultSet rs) throws SQLException;
}
