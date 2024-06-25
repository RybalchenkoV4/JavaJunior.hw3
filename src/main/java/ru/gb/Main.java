package ru.gb;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class Main {
    public static void main(String[] args) {
        // Driver - интерфейс, в котором прописано взаимодействие с БД
        // org.h2.Driver

        // DriverManager - класс, в котором зарегистрированы все драйверы

        // Connection - интерфейс, отвечающий за соединение
        // Statement - интерфейс, отвечающий за запрос в сторону БД
        // PreparedStatement - расширяет Statement

        //jdbc:postgres:user@password:host:port///
        try(Connection connection = DriverManager.getConnection("jdbc:h2:mem:test")) {
            int departQuantity = 5;
            createTable(connection);
            insertData(connection, departQuantity);

            updateData(connection);
            selectDataPerson(connection);
            String age = "30";
            System.out.println("Person(age " + age + ") = " + selectNamesByAge(connection, age));

            System.out.println(getPersonDepartName(connection, 6));

            System.out.println(getPersonDepartments(connection));

            System.out.println(getDepartmentPersons(connection));

        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    private static void createTable(Connection connection) throws SQLException {
        //person(long id, String name, int age, boolean active)
        try(Statement statement = connection.createStatement()){
            statement.execute("""
            create table person(
                id bigint primary key,
                name varchar(256),
                age integer,
                department bigint,
                active boolean
            )
            """);
        } catch (SQLException e) {
            System.err.println("Во время создания таблицы произошла ошибка: " + e.getMessage());
            throw e;
        }

        try (Statement statement = connection.createStatement()) {
            statement.execute("""
            create table department(
                id bigint primary key,
                name varchar(128) not null
            )
            """);
        } catch (SQLException e) {
            System.err.println("Во время создания таблицы произошла ошибка: " + e.getMessage());
            throw e;
        }
    }

    private static void insertData(Connection connection, int quantity) throws SQLException {

        try (Statement statement = connection.createStatement()) {
            StringBuilder insertQuery = new StringBuilder("insert into department(id, name) values\n");
            for (int i = 1; i <= quantity; i++) {
                int id = ThreadLocalRandom.current().nextInt(10, 50);
                insertQuery.append(String.format("(%s, '%s')", i, "Department #" + id));

                if(i != quantity) {
                    insertQuery.append(",\n");
                }
            }

            int insertCount = statement.executeUpdate(insertQuery.toString());
            System.out.println("Вставлено строк департаментов: " + insertCount);

        }

        try (Statement statement = connection.createStatement()) {
            StringBuilder insertQuery = new StringBuilder("insert into person(id, name, age, department, active) values\n");
            for (int i = 1; i <= 10; i++) {
                int age = ThreadLocalRandom.current().nextInt(20, 60);
                boolean active = ThreadLocalRandom.current().nextBoolean();
                int department = ThreadLocalRandom.current().nextInt(1, quantity);

                insertQuery.append(String.format("(%s, '%s', %s, %s, %s)", i, "Person #" + i, age, department, active));

                if(i != 10) {
                    insertQuery.append(",\n");
                }
            }

            int insertCount = statement.executeUpdate(insertQuery.toString());
            System.out.println("Вставлено строк персон: " + insertCount);
        }


    }

    private static void updateData(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()){
            int updateCount = statement.executeUpdate("update person set active = true where id > 5");
            System.out.println("Обновлено строк: " + updateCount);
        }
    }

    private static List<String> selectNamesByAge(Connection connection, String age) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("select name from person where age = ?")) {

            statement.setInt(1, Integer.parseInt(age));
            ResultSet resultSet = statement.executeQuery();

            List<String> names = new ArrayList<>();
            while (resultSet.next()) {
                names.add(resultSet.getString("name"));
            }

            return names;
        }
    }

    private static void selectDataPerson(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()){
            ResultSet resultSet = statement.executeQuery(""" 
                    select id, name, age, department
                    from person
                    where active is true
                   """);

            while (resultSet.next()) {
                long id = resultSet.getLong("id");
                String name = resultSet.getString("name");
                int age = resultSet.getInt("age");
                long department = resultSet.getLong("department");
                // persons.add(new Person(id, name, age);
                System.out.println("Найдена строка: [id = " + id + ", name = " + name + ", age = " + age + ", department = " + department + "]");
            }
        }
    }

    private static String getPersonDepartName(Connection connection, long personId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("select d.name from department d join person p on d.id = department where p.id = ?")) {
            statement.setLong(1, personId);
            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {
                String departId = resultSet.getString("name");
                return String.format("Person(id %s) from %s", personId, departId);
            } else {
                return null;
            }
        }
    }

    private static Map<String, String> getPersonDepartments(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                select p.name as person_name, d.name as department_name
                from person p join department d on p.department = d.id
                """)){
            ResultSet resultSet = statement.executeQuery();
            Map<String, String> map = new HashMap<>();
            while (resultSet.next()) {
                String perName = resultSet.getString("person_name");
                String departName = resultSet.getString("department_name");
                map.put(perName, departName);
            }
            return map;
        }
    }

    private static Map<String, List<String>> getDepartmentPersons(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                select d.name as department_name, p.name as person_name
                from department d join person p on d.id = p.department
                """)){
            ResultSet resultSet = statement.executeQuery();
            Map<String, List<String>> map = new HashMap<>();
            while (resultSet.next()) {
                String departName = resultSet.getString("department_name");
                String personName = resultSet.getString("person_name");
                map.computeIfAbsent(departName, k -> new ArrayList<>()).add(personName);
            }
            return map;
        }
    }
}