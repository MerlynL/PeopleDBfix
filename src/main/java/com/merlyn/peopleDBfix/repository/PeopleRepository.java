package com.merlyn.peopleDBfix.repository;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;


import com.merlyn.peopleDBfix.annotation.SQL;
import com.merlyn.peopleDBfix.model.CRUDOperation;
import com.merlyn.peopleDBfix.model.Person;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;

    public class PeopleRepository extends CRUDRepository<Person> {
        public static final String SAVE_PERSON_SQL = "INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB) VALUES(?, ?, ?)";
        public static final String FIND_BY_ID_SQL = "SELECT ID, FIRST_NAME, LAST_NAME, DOB, SALARY FROM PEOPLE WHERE ID=?";
        public static final String FIND_ALL_SQL = "SELECT ID, FIRST_NAME, LAST_NAME, DOB FROM PEOPLE?";
        public static final String SELECT_COUNT_SQL = "SELECT COUNT(*) FROM PEOPLE";
        public static final String DELETE_SQL = "DELETE FROM PEOPLE WHERE ID=?";
        public static final String DELETE_IN_SQL = "DELETE FROM PEOPLE WHERE ID IN (: ids)";
        public static final String UPDATE_SQL = "UPDATE PEOPLE SET FIRST_NAME=?, LAST_NAME=?, DOB=?, SALARY=? WHERE ID=?";


        public PeopleRepository(Connection connection) {

            super(connection);
        }


        @Override
        @SQL(value = "INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB) VALUES(?, ?, ?)", operationType = CRUDOperation.SAVE)
        void mapForSave(Person entity, PreparedStatement ps) throws SQLException {
            ps.setString(1, entity.getFirstName());
            ps.setString(2, entity.getLastName());
            ps.setTimestamp(3, convertDoBToTimestamp(entity.getDob()));
        }

        @Override
        @SQL(value = "UPDATE PEOPLE SET FIRST_NAME=?, LAST_NAME=?, DOB=?, SALARY=? WHERE ID=?", operationType = CRUDOperation.UPDATE)
        void mapForUpdate(Person entity, PreparedStatement ps) throws SQLException {
            ps.setString(1, entity.getFirstName());
            ps.setString(2, entity.getLastName());
            ps.setTimestamp(3, convertDoBToTimestamp(entity.getDob()));
            ps.setBigDecimal(4, entity.getSalary());
        }


        @Override
        @SQL(value = FIND_BY_ID_SQL, operationType = CRUDOperation.FIND_BY_ID)
        @SQL(value = FIND_ALL_SQL, operationType = CRUDOperation.FIND_ALL)
        @SQL(value = SELECT_COUNT_SQL, operationType = CRUDOperation.COUNT)
        @SQL(value = DELETE_SQL, operationType = CRUDOperation.DELETE_ONE)
        Person extractEntityFromResultSet(ResultSet rs) throws SQLException {
            long PersonId = rs.getLong("ID");
            String firstName = rs.getString("FIRST_NAME");
            String lastName = rs.getString("LAST_NAME");
            ZonedDateTime dob = ZonedDateTime.of(rs.getTimestamp("DOB").toLocalDateTime(), ZoneId.of("+0"));
            BigDecimal salary = rs.getBigDecimal("SALARY");
            return new Person(PersonId, firstName,lastName, dob, salary);
        }

        private Timestamp convertDoBToTimestamp(ZonedDateTime dob) {
            return Timestamp.valueOf(dob.withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime());
        }
    }

