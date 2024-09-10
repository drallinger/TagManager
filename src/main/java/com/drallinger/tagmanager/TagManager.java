package com.drallinger.tagmanager;

import com.drallinger.sqlite.SQLiteFunction;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.stream.Collectors;

public class TagManager<T> implements AutoCloseable{
    private final String objectTableName;
    private final String[] objectColumns;
    private final String groupByColumn;
    private final String orderByColumn;
    private final SQLiteFunction<T> createObjectFunction;
    private final Connection connection;
    private final HashMap<String, PreparedStatement> preparedStatements;

    public TagManager(String databaseFile, String objectTableName, String[] objectColumns, String groupByColumn, String orderByColumn, SQLiteFunction<T> createObjectFunction){
        this.objectTableName = objectTableName;
        this.objectColumns = objectColumns;
        this.groupByColumn = groupByColumn;
        this.orderByColumn = orderByColumn;
        this.createObjectFunction = createObjectFunction;
        connection = getConnection(databaseFile);

        try{
            Statement statement = connection.createStatement();
            statement.execute("""
                CREATE TABLE IF NOT EXISTS tags (
                    name TEXT NOT NULL
                );
                """);
            statement.execute("""
                CREATE TABLE IF NOT EXISTS tag_assignments (
                    tag_id INTEGER NOT NULL,
                    object_id INTEGER NOT NULL
                );
                """);
        }catch (SQLException e){
            System.err.printf("Failed to create database tables: %s%n", e.getMessage());
            System.exit(1);
        }

        preparedStatements = new HashMap<>();
        try{
            preparedStatements.put("createTag", connection.prepareStatement("INSERT INTO tags (name) VALUES (?);", Statement.RETURN_GENERATED_KEYS));
            preparedStatements.put("renameTag", connection.prepareStatement("UPDATE tags SET name=? WHERE rowid=?;"));
            preparedStatements.put("deleteTag", connection.prepareStatement("DELETE FROM tags WHERE rowid=?;"));
            preparedStatements.put("createTagAssignment", connection.prepareStatement("INSERT INTO tag_assignments (tag_id, object_id) VALUES (?, ?);"));
            preparedStatements.put("deleteTagAssignment", connection.prepareStatement("DELETE FROM tag_assignments WHERE tag_id=? AND object_id=?;"));
            preparedStatements.put("deleteTagAssignmentsByTag", connection.prepareStatement("DELETE FROM tag_assignments WHERE tag_id=?;"));
            preparedStatements.put("deleteTagAssignmentsByObject", connection.prepareStatement("DELETE FROM tag_assignments WHERE object_id=?;"));
            preparedStatements.put("getTagsForObject", connection.prepareStatement("SELECT t.rowid, t.name FROM tags AS t INNER JOIN tag_assignments AS ta ON ta.tag_id=t.rowid WHERE ta.object_id=? ORDER BY t.name;"));
            preparedStatements.put("tagExists", connection.prepareStatement("SELECT EXISTS(SELECT 1 FROM tags WHERE name=?);"));
            preparedStatements.put("tagAssignmentExists", connection.prepareStatement("SELECT EXISTS(SELECT 1 FROM tag_assignments WHERE tag_id=? AND object_id=?);"));
            preparedStatements.put("getTagByID", connection.prepareStatement("SELECT rowid, name FROM tags WHERE rowid=?;"));
            preparedStatements.put("getTagByName", connection.prepareStatement("SELECT rowid, name FROM tags WHERE name=?;"));
        }catch (SQLException e){
            System.err.printf("Failed to set up prepared statements: %s%n", e.getMessage());
            System.exit(1);
        }
    }

    public Tag createTag(String tagName){
        Tag tag = null;
        try{
            PreparedStatement preparedStatement = preparedStatements.get("createTag");
            preparedStatement.setString(1, tagName);
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getGeneratedKeys();
            tag = new Tag(resultSet.getInt(1), tagName);
            resultSet.close();
        }catch (SQLException e){
            System.err.printf("Failed to create tag: %s%n", e.getMessage());
            System.exit(1);
        }
        return tag;
    }

    public Tag renameTag(Tag tag, String newName){
        Tag newTag = null;
        try{
            PreparedStatement preparedStatement = preparedStatements.get("renameTag");
            preparedStatement.setString(1, newName);
            preparedStatement.setInt(2, tag.ID());
            preparedStatement.execute();
            newTag = new Tag(tag.ID(), newName);
        }catch (SQLException e){
            System.err.printf("Failed to rename tag: %s%n", e.getMessage());
            System.exit(1);
        }
        return newTag;
    }

    public void deleteTag(Tag tag){
        try{
            PreparedStatement preparedStatement = preparedStatements.get("deleteTag");
            preparedStatement.setInt(1, tag.ID());
            preparedStatement.execute();
        }catch (SQLException e){
            System.err.printf("Failed to delete tag: %s%n", e.getMessage());
            System.exit(1);
        }
    }

    public void createTagAssignment(Tag tag, Taggable taggable){
        try{
            PreparedStatement preparedStatement = preparedStatements.get("createTagAssignment");
            preparedStatement.setInt(1, tag.ID());
            preparedStatement.setInt(2, taggable.getID());
            preparedStatement.execute();
        }catch (SQLException e){
            System.err.printf("Failed to create tag assignment: %s%n", e.getMessage());
            System.exit(1);
        }
    }

    public void deleteTagAssignment(Tag tag, Taggable taggable){
        try{
            PreparedStatement preparedStatement = preparedStatements.get("deleteTagAssignment");
            preparedStatement.setInt(1, tag.ID());
            preparedStatement.setInt(2, taggable.getID());
            preparedStatement.execute();
        }catch (SQLException e){
            System.err.printf("Failed to delete tag assignment: %s%n", e.getMessage());
            System.exit(1);
        }
    }

    public void deleteTagAssignmentsByTag(Tag tag){
        try{
            PreparedStatement preparedStatement = preparedStatements.get("deleteTagAssignmentsByTag");
            preparedStatement.setInt(1, tag.ID());
            preparedStatement.execute();
        }catch (SQLException e){
            System.err.printf("Failed to delete tag assignment by tag: %s%n", e.getMessage());
            System.exit(1);
        }
    }

    public void deleteTagAssignmentsByObject(Taggable taggable){
        try{
            PreparedStatement preparedStatement = preparedStatements.get("deleteTagAssignmentsByObject");
            preparedStatement.setInt(1, taggable.getID());
            preparedStatement.execute();
        }catch (SQLException e){
            System.err.printf("Failed to delete tag assignment by object: %s%n", e.getMessage());
            System.exit(1);
        }
    }

    public ArrayList<Tag> getAllTags(){
        ArrayList<Tag> tags = null;
        try{
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT rowid, name FROM tags ORDER BY name;");
            tags = getTagsFromResultSet(resultSet);
            resultSet.close();
        }catch (SQLException e){
            System.err.printf("Failed to get all tags: %s%n", e.getMessage());
            System.exit(1);
        }
        return tags;
    }

    public ArrayList<TagCount> getAllTagsByCount(){
        ArrayList<TagCount> tags = new ArrayList<>();
        try{
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                "SELECT t.rowid, t.name, COUNT(ta.object_id) FROM tags AS t LEFT JOIN tag_assignments AS ta ON ta.tag_id=t.rowid GROUP BY t.name ORDER BY COUNT(ta.object_id) DESC, t.name;"
            );
            while(resultSet.next()){
                Tag tag = new Tag(resultSet.getInt(1), resultSet.getString(2));
                tags.add(new TagCount(tag, resultSet.getInt(3)));
            }
            resultSet.close();
        }catch (SQLException e){
            System.err.printf("Failed to get all tags: %s%n", e.getMessage());
            System.exit(1);
        }
        return tags;
    }

    public ArrayList<Tag> getTagsForObject(Taggable taggable){
        ArrayList<Tag> tags = null;
        try{
            PreparedStatement preparedStatement = preparedStatements.get("getTagsForObject");
            preparedStatement.setInt(1, taggable.getID());
            ResultSet resultSet = preparedStatement.executeQuery();
            tags = getTagsFromResultSet(resultSet);
            resultSet.close();
        }catch (SQLException e){
            System.err.printf("Failed to get all tags: %s%n", e.getMessage());
            System.exit(1);
        }
        return tags;
    }

    public boolean tagExists(String tagName){
        boolean tagExists = false;
        try{
            PreparedStatement preparedStatement = preparedStatements.get("tagExists");
            preparedStatement.setString(1, tagName);
            ResultSet resultSet = preparedStatement.executeQuery();
            if(resultSet.next()){
                int result = resultSet.getInt(1);
                tagExists = result == 1;
            }
            resultSet.close();
        }catch (SQLException e){
            System.err.printf("Failed to check if tag exists: %s%n", e.getMessage());
            System.exit(1);
        }
        return tagExists;
    }

    public boolean tagAssignmentExists(Tag tag, Taggable taggable){
        boolean tagAssignmentExists = false;
        try{
            PreparedStatement preparedStatement = preparedStatements.get("tagAssignmentExists");
            preparedStatement.setInt(1, tag.ID());
            preparedStatement.setInt(2, taggable.getID());
            ResultSet resultSet = preparedStatement.executeQuery();
            if(resultSet.next()){
                int result = resultSet.getInt(1);
                tagAssignmentExists = result == 1;
            }
            resultSet.close();
        }catch (SQLException e){
            System.err.printf("Failed to check if tag assignment exists: %s%n", e.getMessage());
            System.exit(1);
        }
        return tagAssignmentExists;
    }

    public ArrayList<T> executeTagSearch(TagSearch tagSearch){
        ArrayList<T> objects = new ArrayList<>();
        String query;
        ArrayList<Tag> includedTags = tagSearch.getIncludedTags();
        ArrayList<Tag> excludedTags = tagSearch.getExcludedTags();
        StringBuilder queryBuilder;
        if(!includedTags.isEmpty() || !excludedTags.isEmpty()){
            queryBuilder = new StringBuilder("SELECT o.rowid,");
            for(String objectColumn : objectColumns){
                queryBuilder.append("o.").append(objectColumn).append(",");
            }
            queryBuilder.deleteCharAt(queryBuilder.lastIndexOf(",")).append(" FROM tag_assignments AS ta ");
            StringBuilder whereBuilder = new StringBuilder("WHERE ");
            for(int i = 0; i < includedTags.size(); i++){
                queryBuilder.append("INNER JOIN tag_assignments AS t").append(i).append(" ON ta.object_id=t").append(i).append(".object_id ");
                whereBuilder.append("t").append(i).append(".tag_id=").append(includedTags.get(i).ID()).append(" AND ");
            }
            if(!excludedTags.isEmpty()){
                String excludedTagsString = excludedTags.stream().map(t -> String.valueOf(t.ID())).collect(Collectors.joining(","));
                whereBuilder.append("NOT EXISTS (SELECT 1 FROM tag_assignments AS ta WHERE ta.object_id=o.rowid AND ta.tag_id IN (").append(excludedTagsString).append(")) AND ");
            }
            queryBuilder.append("INNER JOIN ").append(objectTableName).append(" AS o ON o.rowid=ta.object_id ");
            whereBuilder.delete(whereBuilder.lastIndexOf("AND"), whereBuilder.length());
            queryBuilder.append(whereBuilder);
            queryBuilder.append("GROUP BY o.").append(groupByColumn).append(" ORDER BY o.").append(orderByColumn).append(";");
        }else{
            queryBuilder = new StringBuilder("SELECT rowid,");
            for(String objectColumn : objectColumns){
                queryBuilder.append(objectColumn).append(",");
            }
            queryBuilder.deleteCharAt(queryBuilder.lastIndexOf(",")).append(" FROM ").append(objectTableName).append(" ORDER BY ").append(orderByColumn).append(";");
        }
        query = queryBuilder.toString();
        try{
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);
            while(resultSet.next()){
                objects.add(createObjectFunction.execute(resultSet));
            }
            resultSet.close();
        }catch (SQLException e){
            System.err.printf("Failed to search database using tags: %s%n", e.getMessage());
        }
        return objects;
    }

    public ArrayList<T> getObjectsWithoutTags(){
        ArrayList<T> objects = new ArrayList<>();
        StringBuilder queryBuilder = new StringBuilder("SELECT o.rowid,");
        for(String objectColumn : objectColumns){
            queryBuilder.append("o.").append(objectColumn).append(",");
        }
        queryBuilder.deleteCharAt(queryBuilder.lastIndexOf(","))
            .append(" FROM ").append(objectTableName)
            .append(" AS o WHERE NOT EXISTS (SELECT 1 FROM tag_assignments AS ta WHERE o.rowid = ta.object_id) ORDER BY o.")
            .append(orderByColumn).append(";");
        String query = queryBuilder.toString();
        try{
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);
            while(resultSet.next()){
                objects.add(createObjectFunction.execute(resultSet));
            }
            resultSet.close();
        }catch (SQLException e){
            System.err.printf("Failed to get objects without tags: %s%n", e.getMessage());
        }
        return objects;
    }

    public Tag getTagByID(int ID){
        Tag tag = null;
        try{
            PreparedStatement preparedStatement = preparedStatements.get("getTagByID");
            preparedStatement.setInt(1, ID);
            ResultSet resultSet = preparedStatement.executeQuery();
            if(resultSet.next()){
                tag = new Tag(resultSet.getInt(1), resultSet.getString(2));
            }
            resultSet.close();
        }catch (SQLException e){
            System.err.printf("Failed to get tag by ID: %s%n", e.getMessage());
            System.exit(1);
        }
        return tag;
    }

    public Tag getTagByName(String tagName){
        Tag tag = null;
        try{
            PreparedStatement preparedStatement = preparedStatements.get("getTagByName");
            preparedStatement.setString(1, tagName);
            ResultSet resultSet = preparedStatement.executeQuery();
            if(resultSet.next()){
                tag = new Tag(resultSet.getInt(1), resultSet.getString(2));
            }
            resultSet.close();
        }catch (SQLException e){
            System.err.printf("Failed to get tag by name: %s%n", e.getMessage());
            System.exit(1);
        }
        return tag;
    }

    @Override
    public void close(){
        try{
            Set<String> keys = preparedStatements.keySet();
            for(String key : keys){
                preparedStatements.get(key).close();
            }
            connection.close();
        }catch (SQLException e){
            System.err.printf("Failed to close database: %s%n", e.getMessage());
            System.exit(1);
        }
    }

    private Connection getConnection(String databaseFile){
        Connection connection = null;
        try{
            connection = DriverManager.getConnection("jdbc:sqlite:" + databaseFile);
        }catch (SQLException e){
            System.err.printf("Failed to create connection to database: %s%n", e.getMessage());
            System.exit(1);
        }
        return connection;
    }

    private ArrayList<Tag> getTagsFromResultSet(ResultSet resultSet) throws SQLException{
        ArrayList<Tag> tags = new ArrayList<>();
        while(resultSet.next()){
            tags.add(new Tag(resultSet.getInt(1), resultSet.getString(2)));
        }
        return tags;
    }
}
