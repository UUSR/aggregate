package com.aggregate.brain.my;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * Created by uusr on 09.03.16.
 */
public class MyBrain extends AbstractVerticle {
	private String myUrl;
	private String myLogin;
	private String myPassword;
	
	private String Question;
	private String Answer;
	
    @Override
    public void start(Future<Void> f) throws Exception {
    	myUrl = config().getString("url");
        myLogin = config().getString("login");
        myPassword = config().getString("password");
    	if (myUrl == null || myUrl.isEmpty() || myLogin == null || myLogin.isEmpty() || myPassword == null || myPassword.isEmpty()) {
        f.fail("Укажите свой url, login, password");
            return;
    	}
    	
        vertx.eventBus().consumer("asr.result", msg -> {
            Question = (String) msg.body();
            System.out.println("Вопрос = " + Question);
                
        });
        
        /*         */
        vertx.eventBus().consumer("response", msg -> {
            
            if (msg instanceof Number) {
            	Answer = (String) msg.body();
        	    System.out.println("Ответ = Число = " + Answer);
            }
            if (msg instanceof Object) {
            	JsonObject json = new JsonObject();
                json = (JsonObject) msg.body();
            	//Object speeches = json.getJsonArray("speeches");
            	//Answer = speeches.toString();
            	Answer = json.getJsonArray("speeches").toString();
            	System.out.println("Получен объект = " + json);
            	System.out.println("Ответ = " + Answer);            	
            }
        exeption();    
        });
        
        vertx.eventBus().consumer("cmd.mybrain", msg -> {
        	System.out.println("Комманда от Агрегата получена = " + msg);
        	
        });
        
        f.complete();
    }
       
    public synchronized void exeption() {
        if (Question == null || Answer == null) {
       
        return;        
        }
        System.out.println("Данные для базы получены");
    	post();
    }
    
    public synchronized void post() {
    	java.util.Date myDate = new java.util.Date();
    	java.util.Date myTime = new java.util.Date();
    	java.sql.Date sqlDate = new java.sql.Date(myDate.getTime());
    	java.sql.Time sqlTime = new java.sql.Time(myTime.getTime());    	            	
    	try {
        	 Connection con = getConnection();
			 PreparedStatement posted = con.prepareStatement("INSERT INTO mybrain"
        	 		+"(question, answer, Date, Time)"
        	 		+ "VALUES (?, ?, ?, ?)");
			 posted.setString(1, Question);
			 posted.setString(2, Answer);
			 posted.setDate(3, sqlDate);
			 posted.setTime(4, sqlTime);
        	 posted.executeUpdate();
        }  catch(Exception e){System.out.println(e);}
        finally {
        	 System.out.println("Запись добавлена");
        }
    }
    
    public Connection getConnection() throws Exception {
        try{
        	String Driver = "com.mysql.jdbc.Driver";
        	Class.forName(Driver);
        	Connection conn = DriverManager.getConnection(myUrl, myLogin, myPassword);
        	System.out.println("Подключено");
        	return conn;
        	}catch(Exception e ){System.out.println(e);}
        
	    return null;	
	}
}    
    