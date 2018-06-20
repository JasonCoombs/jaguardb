
Windows 环境下捷豹数据库客户端开发指南

1. 捷豹数据库支持JAVA JDBC 编程接口， 在软件包含有JAR文件，和DLL文件

2. 建议预先安装JAVA1.8， JAVAC编译器，和JAVA虚拟机软件

3. 建议将随软件包带的JaguarClient.dll 文件拷贝到系统 C:\Windows\System32\ 目录内
   （注意安装新版本时也需要更新）

4. JaguarJDBCTest.java 是一个利用JDBC接口与捷豹数据库写入和查询数据的演示程序。

5. compilerun.bat 演示如何编译和执行JAVA程序

6. 如果JAR不在当前路径，请在 -cp <...> 里面指定JAR路径和JAR文件本身名称。

7. 在JDBC例子里面DataSource 和 DriverManager 两种方式均可连接到捷豹数据库。用户只需采用任一种方式即可。

8. 注意如果是写入数据(Insert), 更新数据(Update)， 删除数据(Delete)， 请使用 statement.executeUpdate() 语句,
   并且不用后续循环语句（while loop）。

9. 如果是查询(Select)数据（没有数据、一条或多条数据），请使用 statement.executeQuery() 语句, 然后用循环语句
   ResultSet.next() 读取完所有数据， 否则程序可能会停止运行。
 
10. 捷豹JDBC还支持 metadata （数据类型）的查询。