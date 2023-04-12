package com.merlyn.peopleDBfix.annotation;

import com.merlyn.peopleDBfix.model.CRUDOperation;

import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
@Repeatable(MultiSQL.class)
public @interface SQL {
    String value();
    CRUDOperation operationType();

}