package com.vill

import com.google.gson.*
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.PrintWriter
import java.net.ServerSocket
import java.net.Socket
import java.sql.DriverManager
import java.sql.Statement

class Server {
    private val serverSocket = ServerSocket(2333)
    fun start()
    {
        while (true)
        {
            val socket = serverSocket.accept()
            println("IP is ${socket.localAddress}")
            val readThread = ReadThread(socket)
            readThread.start()
        }
    }

    class ReadThread(socket : Socket) : Thread()
    {
        private val databaseUrl = "jdbc:mysql://localhost:3306/spider"
        private val username = "scrapy"
        private val password = "hao5jx"
        private val socket = socket
        private var conn = DriverManager.getConnection(databaseUrl,username,password)
        private var sql: String?= null
        override fun run() {
            val inputStream = socket.getInputStream()!!
            val inputStreamReader = InputStreamReader(inputStream)
            val bufferedReader = BufferedReader(inputStreamReader)
            var msg : String ?
            val outputStream = socket.getOutputStream()
            val printWriter = PrintWriter(outputStream)
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306",username,password)
            var statement = conn.createStatement()
            var terminal = true
            while (terminal)
            {
                var i: Int
                   msg = bufferedReader.readLine()
                println("msg is $msg")
                if (msg != null)
                {
                    var table: String?
                    val json = JsonParser().parse(msg).asJsonObject
                    val keywords = json.getAsJsonObject("keywords")
                    val count = json.get("count").asInt

                    val length = keywords.toString().split(",").size

                    var title: String
                    var url: String
                    var location:String
                    var channel:String
                    var pubDate:String

                    var resultJson = JsonObject()
                    var responseJson: JsonObject?

                    var keywordsList = StringBuffer()
                    for(i in 1..length)
                    {
                        keywordsList.append(keywords.get("$i").asString+".")
                    }
                    var keyword = keywordsList.toString()
                    keyword = keyword.substring(0,keyword.length-1)
                    sql = "select * from spider.query where word='${keyword.replace("."," ")}'"
                    println(sql)
                    var resultSet = statement.executeQuery(sql)
                    if (resultSet.next())
                    {
                        responseJson = JsonObject()
                        responseJson.addProperty("head","villhahaha")
                        responseJson.addProperty("description","begin")
                        printWriter.write("$responseJson\n")
                        printWriter.flush()
                        responseJson = JsonObject()
                        table = resultSet.getString("tablename")
                        sql = "select * from spider.`$table`"
                        resultSet = statement.executeQuery(sql)
                        i = 1
                        while (resultSet.next())
                        {
                            title = resultSet.getString("title")
                            url = resultSet.getString("url")
                            location = resultSet.getString("location")
                            channel = resultSet.getString("channel")
                            pubDate = resultSet.getString("pubdate")

                            var jsonArray = JsonArray()
                            jsonArray.add("$title")
                            jsonArray.add("$url")
                            jsonArray.add("$location")
                            jsonArray.add("$channel")
                            jsonArray.add("$pubDate")

                            resultJson.addProperty("$i","$jsonArray")
                            i++
                        }
                        responseJson.addProperty("head","villhahaha")
                        responseJson.addProperty("description","scrapy_result")
                        responseJson.addProperty("result","$resultJson")
                        responseJson.addProperty("end","end")
                        printWriter.write("$responseJson\n")
                        printWriter.flush()
                        println("$responseJson")
                    }
                    else
                    {
                        val command = "./scrapy.sh $keyword,$count"
                        println(command)
                        Thread{
                            kotlin.run {
                                Runtime.getRuntime().exec(command).waitFor()
                            }
                        }.start()
                        var flag = true
                        var preCompleted = 0
                        while (flag)
                        {
                            var state = waitState(keyword,statement)
                            if (state == 1)
                            {
                                var tableName = getTableName(keyword,statement)
                                sql = "select * from spider.`$tableName`"
                                println(sql)
                                resultSet = statement.executeQuery(sql)
                                responseJson = JsonObject()
                                responseJson.addProperty("head","villhahaha")
                                responseJson.addProperty("description","begin")
                                printWriter.write("$responseJson\n")
                                printWriter.flush()
                                i = 1
                                while (resultSet.next())
                                {
                                    title = resultSet.getString("title")
                                    url = resultSet.getString("url")
                                    location = resultSet.getString("location")
                                    channel = resultSet.getString("channel")
                                    pubDate = resultSet.getString("pubdate")

                                    var jsonArray = JsonArray()
                                    jsonArray.add("$title")
                                    jsonArray.add("$url")
                                    jsonArray.add("$location")
                                    jsonArray.add("$channel")
                                    jsonArray.add("$pubDate")

                                    resultJson.addProperty("${preCompleted + i}","$jsonArray")
                                    i++
                                }
                                //preCompleted = completed
                                responseJson = JsonObject()
                                responseJson.addProperty("head","villhahaha")
                                responseJson.addProperty("description","scrapy_result")
                                responseJson.addProperty("result","$resultJson")
                                if(state == 1)
                                    responseJson.addProperty("end","end")
                                else
                                    responseJson.addProperty("end","heiheihei")
                                printWriter.write("$responseJson\n")
                                printWriter.flush()
                                println(responseJson)
                            }
                            if (state == 1)
                                flag = false
                        }
                    }
                }
                else
                    terminal=false
            }
        }
        private fun waitState(keyword : String, statement : Statement): Int {
            Thread.sleep(550)
            sql = "select * from spider.query where word='$keyword'"
            var resultSet = statement.executeQuery(sql)
            var state = 0
            //var completed = 0
            //var tableName: String? = null
            while (resultSet.next())
            {
                state = resultSet.getInt("state")
                //completed = resultSet.getInt("completed")
                //tableName = resultSet.getString("tableName")
            }
            return state
        }
        private fun getTableName(keyword: String,statement: Statement): String? {
            sql = "select * from spider.query where word='$keyword'"
            var resultSet = statement.executeQuery(sql)
            var state = 0
            //var completed = 0
            var tableName: String? = null
            while (resultSet.next())
            {
                //state = resultSet.getInt("state")
                //completed = resultSet.getInt("completed")
                tableName = resultSet.getString("tableName")
            }
            return tableName
        }
    }

}

fun main(args: Array<String>) {
    val server = Server()
    server.start()
}