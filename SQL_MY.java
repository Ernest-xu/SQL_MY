package CITIC;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
/**
 * Encapsulated SQL method.
 * @author Ernest
 *
 */
public class SQL_MY {
	/**
	 * The insert method of the database returns a self-added ID insert element containing Data that needs to be transformed into a string type.
	 * @param conn
	 * @param str1
	 * @param str2
	 * @param obj
	 * @return
	 * @throws SQLException
	 */
	public static int INSET_sql(Connection conn ,String str1,String str2,Object...obj) throws SQLException{
		int a = 0;
		String sql = "INSERT INTO "+str1+" "+str2;
		PreparedStatement prest=null;
		try{
			prest = conn.prepareStatement(sql,PreparedStatement.RETURN_GENERATED_KEYS);
			for(int i=0;i<obj.length;i++){
				
				prest.setObject(i+1,obj[i]);
			};
			prest.executeUpdate();
			ResultSet rs = prest.getGeneratedKeys();
			if(rs.next());
			a= rs.getInt(1);
			return a;
		}finally{
			if(prest!=null){
				prest.close();
			}
		}
		
		
		
	}
	/**
	 * Handles the database splicing and returns a string.
	 * @param str
	 * @return
	 */
	public static String Joint_sql(String str1,String...str){
		String string="";
		String string1="";
		String string2="";
		if("up".equals(str1)){
			for(int i=0 ;i<str.length;i++){
				if(i==0){
					string1 = string1+str[i]+"=?";
				}else{
					string1 = string1+","+str[i]+"=?";
				}
			}
			string = string1;
			
		}else if("in".equals(str1)){
			for(int i=0;i<str.length;i++){
				if(i==0){
					string1 = string1+str[i];
					string2 = string2+"?";
				}else{
					string1 = string1+","+str[i];
					string2 = string2+",?";
				}
			}
			string = " ("+string1+") values ("+string2+") ";
			
		};
		return string;
		
	}
	/**
	 * Update encapsulated can only implement the modification date of a single database table, 
		and it needs to be written as a String type required for each format.
	 * @param conn
	 * @param str1
	 * @param str2
	 * @param str3
	 * @param obj
	 * @return
	 * @throws SQLException
	 */
	public static int UPDATA_sql(Connection conn ,String str1,String str2,String str3,Object...obj) throws SQLException{
		int a = 0 ;
		String sql = "UPDATE "+str1+" set "+str2+" where "+str3;
		PreparedStatement prest=null;
		try{
			prest=conn.prepareStatement(sql);
			for(int i=0;i<obj.length;i++){
				prest.setObject(i+1, obj[i]);
				a++;
			}
			prest.executeUpdate();
			return a;
		}finally{
			if(prest!=null){
				prest.close();
			}
		}
		
	}
	
}
