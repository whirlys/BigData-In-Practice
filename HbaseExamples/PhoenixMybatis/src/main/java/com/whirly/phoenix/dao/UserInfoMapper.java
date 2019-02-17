package com.whirly.phoenix.dao;

import com.whirly.phoenix.UserInfo;
import org.apache.ibatis.annotations.*;

import java.util.List;

/**
 * @program: HbaseExamples
 * @description:
 * @author: 赖键锋
 * @create: 2019-02-17 17:24
 **/
@Mapper
public interface UserInfoMapper {
    @Insert("upsert into USER_INFO (ID,NAME) VALUES (#{user.id},#{user.name})")
    public void addUser(@Param("user") UserInfo userInfo);

    @Delete("delete from USER_INFO WHERE ID=#{userId}")
    public void deleteUser(@Param("userId") int userId);

    @Select("select * from USER_INFO WHERE ID=#{userId}")
    @ResultMap("userResultMap")
    public UserInfo getUserById(@Param("userId") int userId);

    @Select("select * from USER_INFO WHERE NAME=#{userName}")
    @ResultMap("userResultMap")
    public UserInfo getUserByName(@Param("userName") String userName);

    @Select("select * from USER_INFO")
    @ResultMap("userResultMap")
    public List<UserInfo> getUsers();
}
