//Importing modules

import ballerinax/mongodb;
import ballerina/io;
import ballerinax/kafka;

//binding data to the port and connecting mongodb

mongodb:ConnectionConfig con = {
    host: "localhost",
    port:27017
};

//DataBase Conection and Configuration

string database = "AGN";
mongodb:Client mongoDBClient = check new (con,database);

//Topic details

kafka:ConsumerConfiguration consumerConfig = {
    groupId: "group",
    topics: ["DSA1"]
};

//Inserting Data into the dataBase

string collection = "Customer";
string collection1 = "Orders";
map<json> doc = {"Customer_ID": "1234","name":"King", "LastName":"Hiholike","deliveryAddress":"2081 Khomaasdal"};
map<json> orders = {"order":"Milk","Quantity":12, "customer_Name":"King","cellphone":2468};

//Defining listerner

public listener kafka:Listener kafkaListener = new (kafka:DEFAULT_URL,consumerConfig);



service kafka:Service on kafkaListener {

    //A function to insert data
remote function onConsumerRecord(kafka:Caller caller, kafka:AnydataConsumerRecord[] records) returns error?{
foreach var item in records {
    check mongoDBClient ->insert(orders, collection);


    check mongoDBClient ->insert(doc, collection1);


    io:println(string:fromBytes(<byte[]>item.value));
}


kafka:Error? commitResult = caller->commit();
if commitResult is kafka:Error {


    io:println("Ohh No! an unexpected error error occured");

    
        }
    }
}